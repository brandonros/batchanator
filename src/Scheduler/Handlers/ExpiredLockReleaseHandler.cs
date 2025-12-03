using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Handlers;

public class ExpiredLockReleaseHandler : IScheduledJobHandler
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<ExpiredLockReleaseHandler> _logger;

    public ExpiredLockReleaseHandler(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<ExpiredLockReleaseHandler> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _logger = logger;
    }

    public string Name => "ExpiredLockRelease";
    public string CronExpression => _options.Maintenance.ExpiredLockRelease;

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var now = DateTime.UtcNow;

        var expiredItems = await db.BatchItems
            .Where(i => i.Status == BatchItemStatus.Processing
                        && i.LockedUntil != null
                        && i.LockedUntil < now)
            .ToListAsync(cancellationToken);

        foreach (var item in expiredItems)
        {
            _logger.LogWarning(
                "Releasing expired lock on item {ItemId} (was locked by {LockedBy} until {LockedUntil})",
                item.Id, item.LockedBy, item.LockedUntil);

            item.Status = BatchItemStatus.Failed;
            item.LockedBy = null;
            item.LockedUntil = null;
            item.NextAttemptAt = now;
        }

        if (expiredItems.Count > 0)
        {
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Released {Count} expired locks", expiredItems.Count);
        }
    }
}
