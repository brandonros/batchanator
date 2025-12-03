using System.Text.Json;
using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Entities;
using Batchanator.Core.Enums;
using Medallion.Threading;
using Medallion.Threading.FileSystem;
using Medallion.Threading.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

public class FileIngestionService
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<FileIngestionService> _logger;
    private readonly string? _connectionString;
    private readonly DirectoryInfo? _lockDirectory;

    public FileIngestionService(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        IConfiguration configuration,
        ILogger<FileIngestionService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _logger = logger;

        if (_options.DatabaseProvider == DatabaseProvider.Sqlite)
        {
            var lockDir = string.IsNullOrWhiteSpace(_options.LockDirectory)
                ? Path.Combine(Path.GetTempPath(), "batchanator-locks")
                : _options.LockDirectory;
            _lockDirectory = new DirectoryInfo(lockDir);
            if (!_lockDirectory.Exists)
            {
                _lockDirectory.Create();
            }
            _logger.LogInformation("Using FileSystem locks at {LockDirectory}", _lockDirectory.FullName);
        }
        else
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection")!;
        }
    }

    public async Task<Guid> IngestFileAsync(string filePath, string jobType, CancellationToken cancellationToken = default)
    {
        // Acquire file-level lock to prevent concurrent ingestion of the same file
        var normalizedPath = Path.GetFullPath(filePath);
        var fileLockKey = $"file-ingest:{normalizedPath}";
        var fileLock = CreateLock(fileLockKey);

        await using var fileLockHandle = await fileLock.TryAcquireAsync(
            timeout: TimeSpan.Zero,  // Don't wait, fail fast
            cancellationToken: cancellationToken);

        if (fileLockHandle == null)
        {
            _logger.LogWarning("File {FilePath} is already being ingested by another process", filePath);
            throw new InvalidOperationException($"File '{filePath}' is already being ingested by another process");
        }

        var jobId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var chunkSize = _options.ChunkSize;

        _logger.LogInformation("Starting ingestion for file {FilePath}, JobId: {JobId}", filePath, jobId);

        // Create the job first
        await using (var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken))
        {
            var job = new Job
            {
                Id = jobId,
                Name = Path.GetFileName(filePath),
                JobType = jobType,
                Status = JobStatus.Pending,
                TotalBatches = 0,
                CompletedBatches = 0,
                CreatedAt = now
            };

            dbContext.Jobs.Add(job);
            await dbContext.SaveChangesAsync(cancellationToken);
        }

        // Stream the file and create batches in chunks
        var lineNumber = 0;
        var chunkNumber = 0;
        var currentChunk = new List<(string sourceRowId, string idempotencyKey, string payloadJson)>();

        await foreach (var line in ReadLinesAsync(filePath, cancellationToken))
        {
            lineNumber++;

            // Extract idempotency_key from payload - this is REQUIRED
            var idempotencyKey = ExtractIdempotencyKey(line, lineNumber, jobType);
            currentChunk.Add((lineNumber.ToString(), idempotencyKey, line));

            if (currentChunk.Count >= chunkSize)
            {
                chunkNumber++;
                await CreateBatchWithLockAsync(jobId, jobType, chunkNumber, currentChunk, cancellationToken);
                currentChunk.Clear();
            }
        }

        // Handle remaining items
        if (currentChunk.Count > 0)
        {
            chunkNumber++;
            await CreateBatchWithLockAsync(jobId, jobType, chunkNumber, currentChunk, cancellationToken);
        }

        // Update job status to Processing
        await using (var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken))
        {
            var job = await dbContext.Jobs.FindAsync([jobId], cancellationToken);
            if (job != null)
            {
                job.TotalBatches = chunkNumber;
                job.Status = JobStatus.Processing;
                await dbContext.SaveChangesAsync(cancellationToken);
            }
        }

        _logger.LogInformation("Completed ingestion for JobId: {JobId}. Total batches: {TotalBatches}, Total items: {TotalItems}",
            jobId, chunkNumber, lineNumber);

        return jobId;
    }

    private IDistributedLock CreateLock(string lockKey)
    {
        if (_options.DatabaseProvider == DatabaseProvider.Sqlite)
        {
            return new FileDistributedLock(_lockDirectory!, lockKey);
        }
        return new SqlDistributedLock(lockKey, _connectionString!);
    }

    private async Task CreateBatchWithLockAsync(
        Guid jobId,
        string jobType,
        int sequenceNumber,
        List<(string sourceRowId, string idempotencyKey, string payloadJson)> items,
        CancellationToken cancellationToken)
    {
        var lockKey = $"ingest-{jobId}-chunk-{sequenceNumber}";
        var @lock = CreateLock(lockKey);

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
        var allKeys = items.Select(i => i.idempotencyKey).ToList();
        var existingItems = await dbContext.BatchItems
            .Where(i => allKeys.Contains(i.IdempotencyKey))
            .Select(i => new { i.IdempotencyKey, i.Status })
            .ToDictionaryAsync(i => i.IdempotencyKey, i => i.Status, cancellationToken);

        var itemsToInsert = new List<BatchItem>();
        var skippedCount = 0;

        foreach (var (sourceRowId, idempotencyKey, payloadJson) in items)
        {
            if (existingItems.TryGetValue(idempotencyKey, out var status))
            {
                switch (status)
                {
                    case BatchItemStatus.Succeeded:
                        _logger.LogDebug("Skipping row {Row}: idempotency key {Key} already succeeded",
                            sourceRowId, idempotencyKey);
                        break;
                    case BatchItemStatus.DeadLetter:
                        _logger.LogWarning("Skipping row {Row}: idempotency key {Key} previously dead-lettered",
                            sourceRowId, idempotencyKey);
                        break;
                    default:
                        _logger.LogDebug("Skipping row {Row}: idempotency key {Key} already in progress",
                            sourceRowId, idempotencyKey);
                        break;
                }
                skippedCount++;
                continue;
            }

            itemsToInsert.Add(new BatchItem
            {
                Id = Guid.NewGuid(),
                BatchId = batchId,
                SourceRowId = sourceRowId,
                IdempotencyKey = idempotencyKey,
                PayloadJson = payloadJson,
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

    private static string ExtractIdempotencyKey(string payloadJson, int lineNumber, string jobType)
    {
        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty("idempotency_key", out var keyElement))
            {
                throw new InvalidOperationException(
                    $"Row {lineNumber} missing required 'idempotency_key' field. Every row must include an idempotency_key.");
            }

            var key = keyElement.GetString();
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new InvalidOperationException(
                    $"Row {lineNumber} has empty 'idempotency_key' field. The idempotency_key must be a non-empty string.");
            }

            // Prefix with jobType for namespace separation
            return $"{jobType}:{key}";
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Row {lineNumber} contains invalid JSON: {ex.Message}", ex);
        }
    }

    private static async IAsyncEnumerable<string> ReadLinesAsync(
        string filePath,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(filePath);
        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            if (!string.IsNullOrWhiteSpace(line) && !cancellationToken.IsCancellationRequested)
            {
                yield return line;
            }
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
