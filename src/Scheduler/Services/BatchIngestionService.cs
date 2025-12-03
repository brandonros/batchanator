using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Entities;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

/// <summary>
/// Core ingestion service that handles batching, idempotency, and locking.
/// Used by FileIngestionService, DatabaseIngestionService, and any future ingestion sources.
/// </summary>
public class BatchIngestionService
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly DistributedLockFactory _lockFactory;
    private readonly ILogger<BatchIngestionService> _logger;

    public BatchIngestionService(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        DistributedLockFactory lockFactory,
        ILogger<BatchIngestionService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _lockFactory = lockFactory;
        _logger = logger;
    }

    /// <summary>
    /// Ingests work items into the batch processing system.
    /// Handles chunking, idempotency checks, and distributed locking.
    /// </summary>
    public async Task<Guid> IngestAsync(
        string jobName,
        string jobType,
        IAsyncEnumerable<RawWorkItem> items,
        CancellationToken cancellationToken = default)
    {
        var jobId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var chunkSize = _options.ChunkSize;

        _logger.LogInformation("Starting ingestion for job {JobName}, JobId: {JobId}", jobName, jobId);

        // Create the job first
        await using (var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken))
        {
            var job = new Job
            {
                Id = jobId,
                Name = jobName,
                JobType = jobType,
                Status = JobStatus.Pending,
                TotalBatches = 0,
                CompletedBatches = 0,
                CreatedAt = now
            };

            dbContext.Jobs.Add(job);
            await dbContext.SaveChangesAsync(cancellationToken);
        }

        // Stream items and create batches in chunks
        var itemNumber = 0;
        var chunkNumber = 0;
        var currentChunk = new List<RawWorkItem>();

        await foreach (var item in items.WithCancellation(cancellationToken))
        {
            itemNumber++;
            currentChunk.Add(item);

            if (currentChunk.Count >= chunkSize)
            {
                chunkNumber++;
                await CreateBatchWithLockAsync(jobId, chunkNumber, currentChunk, cancellationToken);
                currentChunk.Clear();
            }
        }

        // Handle remaining items
        if (currentChunk.Count > 0)
        {
            chunkNumber++;
            await CreateBatchWithLockAsync(jobId, chunkNumber, currentChunk, cancellationToken);
        }

        // Update job status to Processing
        await using (var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken))
        {
            var job = await dbContext.Jobs.FindAsync([jobId], cancellationToken);
            if (job != null)
            {
                // Query actual batch count (may differ from chunkNumber if batches were skipped)
                job.TotalBatches = await dbContext.Batches.CountAsync(b => b.JobId == jobId, cancellationToken);
                job.Status = JobStatus.Processing;
                await dbContext.SaveChangesAsync(cancellationToken);
            }
        }

        _logger.LogInformation("Completed ingestion for JobId: {JobId}. Total batches: {TotalBatches}, Total items: {TotalItems}",
            jobId, chunkNumber, itemNumber);

        return jobId;
    }

    private async Task CreateBatchWithLockAsync(
        Guid jobId,
        int sequenceNumber,
        List<RawWorkItem> items,
        CancellationToken cancellationToken)
    {
        var lockKey = $"ingest-{jobId}-chunk-{sequenceNumber}";
        var @lock = _lockFactory.Create(lockKey);

        await using var handle = await @lock.TryAcquireAsync(
            timeout: TimeSpan.FromSeconds(30),
            cancellationToken: cancellationToken);

        if (handle == null)
        {
            _logger.LogWarning("Could not acquire lock for {LockKey}, another instance may be processing", lockKey);
            return;
        }

        var now = DateTime.UtcNow;
        var batchId = Guid.NewGuid();

        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        // Batch check idempotency for all items (single query instead of N queries)
        var allKeys = items.Select(i => i.IdempotencyKey).ToList();
        var existingItems = await dbContext.BatchItems
            .Where(i => allKeys.Contains(i.IdempotencyKey))
            .Select(i => new { i.IdempotencyKey, i.Status })
            .ToDictionaryAsync(i => i.IdempotencyKey, i => i.Status, cancellationToken);

        var itemsToInsert = new List<BatchItem>();
        var skippedCount = 0;

        foreach (var item in items)
        {
            if (existingItems.TryGetValue(item.IdempotencyKey, out var status))
            {
                switch (status)
                {
                    case BatchItemStatus.Succeeded:
                        _logger.LogDebug("Skipping row {Row}: idempotency key {Key} already succeeded",
                            item.SourceRowId, item.IdempotencyKey);
                        break;
                    case BatchItemStatus.DeadLetter:
                        _logger.LogWarning("Skipping row {Row}: idempotency key {Key} previously dead-lettered",
                            item.SourceRowId, item.IdempotencyKey);
                        break;
                    default:
                        _logger.LogDebug("Skipping row {Row}: idempotency key {Key} already in progress",
                            item.SourceRowId, item.IdempotencyKey);
                        break;
                }
                skippedCount++;
                continue;
            }

            itemsToInsert.Add(new BatchItem
            {
                Id = Guid.NewGuid(),
                BatchId = batchId,
                SourceRowId = item.SourceRowId,
                IdempotencyKey = item.IdempotencyKey,
                PayloadJson = item.PayloadJson,
                Status = BatchItemStatus.Pending,
                AttemptCount = 0,
                CreatedAt = now
            });
        }

        // Only create batch if there are items to insert
        if (itemsToInsert.Count == 0)
        {
            _logger.LogInformation("Batch {SequenceNumber} for job {JobId} skipped entirely - all {Count} items already processed",
                sequenceNumber, jobId, items.Count);
            return;
        }

        var batch = new Batch
        {
            Id = batchId,
            JobId = jobId,
            SequenceNumber = sequenceNumber,
            Status = BatchStatus.Pending,
            TotalItems = itemsToInsert.Count,
            SucceededItems = 0,
            FailedItems = 0,
            CreatedAt = now
        };

        dbContext.Batches.Add(batch);
        dbContext.BatchItems.AddRange(itemsToInsert);

        try
        {
            await dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            // Race condition: another process inserted the same idempotency keys
            // This is expected behavior under concurrent ingestion, not a failure
            _logger.LogInformation(
                "Batch {BatchId} had duplicate items detected at save time (concurrent ingestion race)",
                batchId);
            return;
        }

        if (skippedCount > 0)
        {
            _logger.LogInformation("Created batch {BatchId} with {ItemCount} items for job {JobId} (skipped {SkippedCount} already-processed items)",
                batchId, itemsToInsert.Count, jobId, skippedCount);
        }
        else
        {
            _logger.LogDebug("Created batch {BatchId} with {ItemCount} items for job {JobId}",
                batchId, itemsToInsert.Count, jobId);
        }
    }

    private static bool IsUniqueConstraintViolation(DbUpdateException ex)
    {
        // SQL Server: 2627 (unique constraint), 2601 (unique index)
        // SQLite: error 19 (UNIQUE constraint failed)
        return ex.InnerException switch
        {
            Microsoft.Data.SqlClient.SqlException sqlEx =>
                sqlEx.Number == 2627 || sqlEx.Number == 2601,
            Microsoft.Data.Sqlite.SqliteException sqliteEx =>
                sqliteEx.SqliteErrorCode == 19,
            _ => false
        };
    }
}
