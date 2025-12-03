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
    /// Returns null if no work was found.
    /// </summary>
    public async Task<Guid?> IngestPendingWorkAsync(
        string jobType,
        CancellationToken cancellationToken = default)
    {
        // Check if there's any work to do first
        await using var checkContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
        var hasWork = await checkContext.PendingWork
            .AnyAsync(p => p.JobType == jobType, cancellationToken);

        if (!hasWork)
        {
            _logger.LogDebug("No pending work found for job type {JobType}", jobType);
            return null;
        }

        _logger.LogInformation("Found pending work for job type {JobType}, starting ingestion", jobType);

        var items = QueryPendingWorkItems(jobType, cancellationToken);

        return await _batchIngestion.IngestAsync(
            jobName: $"{jobType}-{DateTime.UtcNow:yyyyMMdd-HHmmss}",
            jobType: jobType,
            items: items,
            cancellationToken: cancellationToken);
    }

    private async IAsyncEnumerable<RawWorkItem> QueryPendingWorkItems(
        string jobType,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        // Stream items from database
        var pendingItems = dbContext.PendingWork
            .Where(p => p.JobType == jobType)
            .OrderBy(p => p.CreatedAt)
            .AsAsyncEnumerable();

        await foreach (var item in pendingItems.WithCancellation(cancellationToken))
        {
            // Idempotency key is already namespaced with jobType in PendingWork
            var idempotencyKey = $"{jobType}:{item.IdempotencyKey}";

            yield return new RawWorkItem(
                SourceRowId: item.Id.ToString(),
                IdempotencyKey: idempotencyKey,
                PayloadJson: item.PayloadJson
            );
        }
    }
}
