using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Handlers;

public class DeadLetterRetryHandler : IScheduledJobHandler
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<DeadLetterRetryHandler> _logger;

    public DeadLetterRetryHandler(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<DeadLetterRetryHandler> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _logger = logger;
    }

    public string Name => "DeadLetterRetry";
    public string CronExpression => _options.Maintenance.DeadLetterRetry;

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var cooldownThreshold = DateTime.UtcNow.AddHours(-1);

        var deadLetterItems = await db.BatchItems
            .Where(i => i.Status == BatchItemStatus.DeadLetter
                        && i.UpdatedAt < cooldownThreshold
                        && i.AttemptCount < 10)
            .Take(100)
            .ToListAsync(cancellationToken);

        foreach (var item in deadLetterItems)
        {
            item.Status = BatchItemStatus.Failed;
            item.NextAttemptAt = DateTime.UtcNow;
            _logger.LogInformation(
                "Retrying dead-letter item {ItemId} (attempt {Attempt})",
                item.Id, item.AttemptCount + 1);
        }

        if (deadLetterItems.Count > 0)
        {
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Queued {Count} dead-letter items for retry", deadLetterItems.Count);
        }
    }
}
