using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Handlers;

public class StaleJobCleanupHandler : IScheduledJobHandler
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<StaleJobCleanupHandler> _logger;

    public StaleJobCleanupHandler(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<StaleJobCleanupHandler> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _logger = logger;
    }

    public string Name => "StaleJobCleanup";
    public string CronExpression => _options.Maintenance.StaleJobCleanup;

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var staleThreshold = DateTime.UtcNow.AddHours(-24);

        var staleJobs = await db.Jobs
            .Where(j => j.Status == JobStatus.Processing && j.CreatedAt < staleThreshold)
            .ToListAsync(cancellationToken);

        foreach (var job in staleJobs)
        {
            _logger.LogWarning(
                "Marking stale job {JobId} ({JobName}) as Failed - stuck since {CreatedAt}",
                job.Id, job.Name, job.CreatedAt);

            job.Status = JobStatus.Failed;
            job.CompletedAt = DateTime.UtcNow;
        }

        if (staleJobs.Count > 0)
        {
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Cleaned up {Count} stale jobs", staleJobs.Count);
        }
    }
}
