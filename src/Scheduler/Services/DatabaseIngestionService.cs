using Batchanator.Core;
using Batchanator.Core.Data;
using Microsoft.EntityFrameworkCore;

namespace Scheduler.Services;

/// <summary>
/// Ingests work items from the PendingWork database table into the batch processing system.
/// Delegates core logic to BatchIngestionService.
/// </summary>
public class DatabaseIngestionService
{
    private readonly BatchIngestionService _batchIngestion;
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly ILogger<DatabaseIngestionService> _logger;

    public DatabaseIngestionService(
        BatchIngestionService batchIngestion,
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        ILogger<DatabaseIngestionService> logger)
    {
        _batchIngestion = batchIngestion;
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    /// <summary>
    /// Ingests all pending work items for a specific job type.
    /// Deletes processed rows after successful ingestion.
    /// Returns null if no work was found.
    /// </summary>
    public async Task<Guid?> IngestPendingWorkAsync(
        string jobType,
        CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        // Collect IDs to delete after ingestion (snapshot before streaming)
        var pendingIds = await dbContext.PendingWork
            .Where(p => p.JobType == jobType)
            .Select(p => p.Id)
            .ToListAsync(cancellationToken);

        if (pendingIds.Count == 0)
        {
            _logger.LogDebug("No pending work found for job type {JobType}", jobType);
            return null;
        }

        _logger.LogInformation(
            "Found {Count} pending work items for job type {JobType}, starting ingestion",
            pendingIds.Count, jobType);

        var items = QueryPendingWorkItems(jobType, pendingIds, cancellationToken);

        var jobId = await _batchIngestion.IngestAsync(
            jobName: $"{jobType}-{DateTime.UtcNow:yyyyMMdd-HHmmss}",
            jobType: jobType,
            items: items,
            cancellationToken: cancellationToken);

        // Delete processed rows after successful ingestion
        var deletedCount = await dbContext.PendingWork
            .Where(p => pendingIds.Contains(p.Id))
            .ExecuteDeleteAsync(cancellationToken);

        _logger.LogInformation(
            "Deleted {DeletedCount} pending work rows for job {JobId}",
            deletedCount, jobId);

        return jobId;
    }

    private async IAsyncEnumerable<RawWorkItem> QueryPendingWorkItems(
        string jobType,
        List<Guid> pendingIds,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        // Stream items from database - filter by snapshot IDs to avoid race condition
        // where new rows inserted after snapshot would be ingested but not deleted
        var pendingItems = dbContext.PendingWork
            .Where(p => pendingIds.Contains(p.Id))
            .OrderBy(p => p.CreatedAt)
            .AsAsyncEnumerable();

        await foreach (var item in pendingItems.WithCancellation(cancellationToken))
        {
            // PendingWork stores raw key; namespace with jobType to match BatchItems format
            var idempotencyKey = $"{jobType}:{item.IdempotencyKey}";

            yield return new RawWorkItem(
                SourceRowId: item.Id.ToString(),
                IdempotencyKey: idempotencyKey,
                PayloadJson: item.PayloadJson
            );
        }
    }
}
