using Cronos;
using Scheduler.Handlers;

namespace Scheduler.Services;

public class CronSchedulerService : BackgroundService
{
    private readonly IEnumerable<IScheduledJobHandler> _handlers;
    private readonly DistributedLockFactory _lockFactory;
    private readonly ILogger<CronSchedulerService> _logger;
    private readonly string _workerId;
    private readonly Dictionary<string, DateTime> _lastRunTimes = new();

    public CronSchedulerService(
        IEnumerable<IScheduledJobHandler> handlers,
        DistributedLockFactory lockFactory,
        ILogger<CronSchedulerService> logger)
    {
        _handlers = handlers;
        _lockFactory = lockFactory;
        _logger = logger;
        _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "CronScheduler started. WorkerId: {WorkerId}, Jobs: {JobCount}",
            _workerId, _handlers.Count());

        foreach (var handler in _handlers)
        {
            _logger.LogInformation("  - {JobName}: {CronExpression}", handler.Name, handler.CronExpression);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;

            foreach (var handler in _handlers)
            {
                if (ShouldRunJob(handler, now))
                {
                    _ = TryExecuteJobAsync(handler, stoppingToken);
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }

        _logger.LogInformation("CronScheduler stopped");
    }

    private bool ShouldRunJob(IScheduledJobHandler handler, DateTime now)
    {
        var cron = CronExpression.Parse(handler.CronExpression);
        var lastRun = _lastRunTimes.GetValueOrDefault(handler.Name, now.AddMinutes(-10));

        var nextRun = cron.GetNextOccurrence(lastRun, TimeZoneInfo.Utc);
        return nextRun.HasValue && nextRun.Value <= now;
    }

    private async Task TryExecuteJobAsync(IScheduledJobHandler handler, CancellationToken cancellationToken)
    {
        var lockKey = $"cron-{handler.Name}";
        var @lock = _lockFactory.Create(lockKey);

        await using var lockHandle = await @lock.TryAcquireAsync(
            timeout: TimeSpan.FromSeconds(5),
            cancellationToken: cancellationToken);

        if (lockHandle == null)
        {
            _logger.LogDebug("Skipping job {JobName} - another instance holds the lock", handler.Name);
            return;
        }

        _logger.LogInformation("Acquired lock for job {JobName}, executing on {WorkerId}", handler.Name, _workerId);

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await handler.ExecuteAsync(cancellationToken);
            stopwatch.Stop();

            _lastRunTimes[handler.Name] = DateTime.UtcNow;

            _logger.LogInformation("Job {JobName} completed in {ElapsedMs}ms", handler.Name, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job {JobName} failed", handler.Name);
        }
    }
}
