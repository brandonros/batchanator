using System.Net.Http.Json;
using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Entities;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

public class DispatcherHostedService : BackgroundService
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IOptions<BatchanatorOptions> _options;
    private readonly ILogger<DispatcherHostedService> _logger;
    private readonly string _workerId;

    public DispatcherHostedService(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IHttpClientFactory httpClientFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<DispatcherHostedService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _httpClientFactory = httpClientFactory;
        _options = options;
        _logger = logger;
        _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Dispatcher started. WorkerId: {WorkerId}", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processedCount = await ProcessBatchAsync(stoppingToken);

                if (processedCount == 0)
                {
                    // No work to do, wait before polling again
                    await Task.Delay(
                        TimeSpan.FromSeconds(_options.Value.PollingIntervalSeconds),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in dispatcher loop");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        _logger.LogInformation("Dispatcher stopped");
    }

    private async Task<int> ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var workItems = await ClaimItemsAsync(cancellationToken);

        if (workItems.Count == 0)
        {
            return 0;
        }

        _logger.LogInformation("Claimed {Count} items for processing", workItems.Count);

        var maxConcurrency = _options.Value.MaxConcurrencyPerWorker;
        using var throttler = new SemaphoreSlim(maxConcurrency);

        var tasks = workItems.Select(async item =>
        {
            await throttler.WaitAsync(cancellationToken);
            try
            {
                await ProcessItemAsync(item, cancellationToken);
            }
            finally
            {
                throttler.Release();
            }
        });

        await Task.WhenAll(tasks);

        return workItems.Count;
    }

    private async Task<List<WorkItem>> ClaimItemsAsync(CancellationToken cancellationToken)
    {
        var options = _options.Value;

        if (options.DatabaseProvider == DatabaseProvider.Sqlite)
        {
            return await ClaimItemsSqliteAsync(cancellationToken);
        }

        return await ClaimItemsSqlServerAsync(cancellationToken);
    }

    private async Task<List<WorkItem>> ClaimItemsSqliteAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batchSize = _options.Value.ProcessingBatchSize;
        var lockTimeout = _options.Value.LockTimeoutMinutes;
        var now = DateTime.UtcNow;
        var lockedUntil = now.AddMinutes(lockTimeout);

        // Atomic UPDATE-then-SELECT to prevent race conditions
        var claimToken = $"{_workerId}-{Guid.NewGuid():N}";

        var updateSql = @"
            UPDATE BatchItems
            SET Status = 'Processing',
                LockedBy = @claimToken,
                LockedUntil = @lockedUntil,
                UpdatedAt = @now
            WHERE Id IN (
                SELECT Id FROM BatchItems
                WHERE (Status = 'Pending' OR (Status = 'Failed' AND NextAttemptAt <= @now))
                  AND (LockedUntil IS NULL OR LockedUntil < @now)
                ORDER BY CreatedAt
                LIMIT @batchSize
            )";

        await dbContext.Database.ExecuteSqlRawAsync(
            updateSql,
            new Microsoft.Data.Sqlite.SqliteParameter("@claimToken", claimToken),
            new Microsoft.Data.Sqlite.SqliteParameter("@lockedUntil", lockedUntil),
            new Microsoft.Data.Sqlite.SqliteParameter("@now", now),
            new Microsoft.Data.Sqlite.SqliteParameter("@batchSize", batchSize));

        // Now fetch only the items we successfully claimed
        var items = await dbContext.BatchItems
            .Where(i => i.LockedBy == claimToken)
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        return await MapClaimedItemsToWorkItems(dbContext, items, cancellationToken);
    }

    private async Task<List<WorkItem>> ClaimItemsSqlServerAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batchSize = _options.Value.ProcessingBatchSize;
        var lockTimeout = _options.Value.LockTimeoutMinutes;
        var now = DateTime.UtcNow;

        // Use raw SQL for the claim operation with proper locking hints
        var sql = $@"
            WITH cte AS (
                SELECT TOP ({batchSize}) *
                FROM BatchItems WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE (Status = 'Pending' OR (Status = 'Failed' AND NextAttemptAt <= @now))
                  AND (LockedUntil IS NULL OR LockedUntil < @now)
                ORDER BY CreatedAt
            )
            UPDATE cte
            SET Status = 'Processing',
                LockedBy = @workerId,
                LockedUntil = DATEADD(MINUTE, {lockTimeout}, @now),
                UpdatedAt = @now
            OUTPUT inserted.*;
        ";

        var items = await dbContext.BatchItems
            .FromSqlRaw(sql,
                new Microsoft.Data.SqlClient.SqlParameter("@now", now),
                new Microsoft.Data.SqlClient.SqlParameter("@workerId", _workerId))
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        return await MapClaimedItemsToWorkItems(dbContext, items, cancellationToken);
    }

    private async Task<List<WorkItem>> MapClaimedItemsToWorkItems(
        BatchanatorDbContext dbContext,
        List<BatchItem> items,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
            return [];

        var batchIds = items.Select(i => i.BatchId).Distinct().ToList();
        var batchJobTypes = await dbContext.Batches
            .Where(b => batchIds.Contains(b.Id))
            .Include(b => b.Job)
            .ToDictionaryAsync(b => b.Id, b => b.Job.JobType, cancellationToken);

        return items.Select(item => new WorkItem(
            item.Id,
            item.BatchId,
            item.IdempotencyKey,
            item.PayloadJson,
            batchJobTypes.GetValueOrDefault(item.BatchId, "unknown")
        )).ToList();
    }

    private async Task ProcessItemAsync(WorkItem workItem, CancellationToken cancellationToken)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("BatchanatorApi");
            var request = new ProcessRequest(workItem.JobType, workItem.IdempotencyKey, workItem.PayloadJson);
            var response = await client.PostAsJsonAsync("/process", request, cancellationToken);
            var result = await response.Content.ReadFromJsonAsync<ProcessResponse>(cancellationToken);

            await UpdateItemResultAsync(workItem.ItemId, workItem.BatchId, result!, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing item {ItemId}", workItem.ItemId);

            var errorResult = new ProcessResponse(false, ex.Message, null);
            await UpdateItemResultAsync(workItem.ItemId, workItem.BatchId, errorResult, cancellationToken);
        }
    }

    private async Task UpdateItemResultAsync(Guid itemId, Guid batchId, ProcessResponse result, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var item = await dbContext.BatchItems.FindAsync([itemId], cancellationToken);
        if (item == null) return;

        var now = DateTime.UtcNow;
        var options = _options.Value;

        if (result.Success)
        {
            item.Status = BatchItemStatus.Succeeded;
            item.ResultPayload = result.Result;
            item.LastError = null;
        }
        else
        {
            item.AttemptCount++;
            item.LastError = result.Error;

            if (item.AttemptCount >= options.MaxAttempts)
            {
                item.Status = BatchItemStatus.DeadLetter;
            }
            else
            {
                item.Status = BatchItemStatus.Failed;
                // Exponential backoff: 2^attemptCount seconds
                var backoffSeconds = Math.Pow(2, item.AttemptCount);
                item.NextAttemptAt = now.AddSeconds(backoffSeconds);
            }
        }

        item.LockedBy = null;
        item.LockedUntil = null;
        item.UpdatedAt = now;

        await dbContext.SaveChangesAsync(cancellationToken);

        // Trigger reconciliation check for the batch
        await ReconcileBatchAsync(batchId, cancellationToken);
    }

    private async Task ReconcileBatchAsync(Guid batchId, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batch = await dbContext.Batches
            .Include(b => b.Job)
            .FirstOrDefaultAsync(b => b.Id == batchId, cancellationToken);

        if (batch == null) return;

        // Count items by status
        var statusCounts = await dbContext.BatchItems
            .Where(i => i.BatchId == batchId)
            .GroupBy(i => i.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(cancellationToken);

        var succeeded = statusCounts.FirstOrDefault(x => x.Status == BatchItemStatus.Succeeded)?.Count ?? 0;
        var failed = statusCounts.FirstOrDefault(x => x.Status == BatchItemStatus.DeadLetter)?.Count ?? 0;
        var pending = statusCounts.FirstOrDefault(x => x.Status == BatchItemStatus.Pending)?.Count ?? 0;
        var processing = statusCounts.FirstOrDefault(x => x.Status == BatchItemStatus.Processing)?.Count ?? 0;
        var retrying = statusCounts.FirstOrDefault(x => x.Status == BatchItemStatus.Failed)?.Count ?? 0;

        batch.SucceededItems = succeeded;
        batch.FailedItems = failed;

        var now = DateTime.UtcNow;

        // Check if batch is complete (all items are in terminal state)
        if (pending == 0 && processing == 0 && retrying == 0)
        {
            batch.Status = failed > 0 ? BatchStatus.Failed : BatchStatus.Completed;
            batch.CompletedAt = now;

            _logger.LogInformation("Batch {BatchId} completed. Succeeded: {Succeeded}, Failed: {Failed}",
                batchId, succeeded, failed);

            // Check if job is complete
            await ReconcileJobAsync(batch.JobId, cancellationToken);
        }
        else
        {
            batch.Status = BatchStatus.Processing;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    private async Task ReconcileJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var job = await dbContext.Jobs.FindAsync([jobId], cancellationToken);
        if (job == null) return;

        var batchStatuses = await dbContext.Batches
            .Where(b => b.JobId == jobId)
            .Select(b => b.Status)
            .ToListAsync(cancellationToken);

        var completedBatches = batchStatuses.Count(s => s == BatchStatus.Completed || s == BatchStatus.Failed);
        var allComplete = batchStatuses.All(s => s == BatchStatus.Completed || s == BatchStatus.Failed);
        var anyFailed = batchStatuses.Any(s => s == BatchStatus.Failed);

        job.CompletedBatches = completedBatches;

        if (allComplete)
        {
            job.Status = anyFailed ? JobStatus.PartiallyCompleted : JobStatus.Completed;
            job.CompletedAt = DateTime.UtcNow;

            _logger.LogInformation("Job {JobId} completed with status {Status}", jobId, job.Status);
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}

// Internal records for dispatcher
internal record WorkItem(Guid ItemId, Guid BatchId, string IdempotencyKey, string PayloadJson, string JobType);
internal record ProcessRequest(string JobType, string IdempotencyKey, string Payload);
internal record ProcessResponse(bool Success, string? Error, string? Result);
