using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;

namespace Scheduler.Services;

public class ReconciliationService
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly ILogger<ReconciliationService> _logger;

    public ReconciliationService(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        ILogger<ReconciliationService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    public async Task ReconcileBatchAsync(Guid batchId, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batch = await dbContext.Batches
            .Include(b => b.Job)
            .FirstOrDefaultAsync(b => b.Id == batchId, cancellationToken);

        if (batch == null) return;

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

        if (pending == 0 && processing == 0 && retrying == 0)
        {
            batch.Status = failed > 0 ? BatchStatus.Failed : BatchStatus.Completed;
            batch.CompletedAt = now;

            _logger.LogInformation("Batch {BatchId} completed. Succeeded: {Succeeded}, Failed: {Failed}",
                batchId, succeeded, failed);

            await ReconcileJobAsync(batch.JobId, cancellationToken);
        }
        else
        {
            batch.Status = BatchStatus.Processing;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async Task ReconcileJobAsync(Guid jobId, CancellationToken cancellationToken)
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
