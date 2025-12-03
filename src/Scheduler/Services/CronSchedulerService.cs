using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Cronos;
using Medallion.Threading;
using Medallion.Threading.FileSystem;
using Medallion.Threading.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

/// <summary>
/// Cron-based scheduler that uses distributed locking to prevent
/// double-firing across multiple pods/replicas.
/// </summary>
public class CronSchedulerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<CronSchedulerService> _logger;
    private readonly string _workerId;
    private readonly string? _connectionString;
    private readonly DirectoryInfo? _lockDirectory;

    private readonly List<ScheduledJob> _scheduledJobs;

    public CronSchedulerService(
        IServiceProvider serviceProvider,
        IOptions<BatchanatorOptions> options,
        IConfiguration configuration,
        ILogger<CronSchedulerService> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
        _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";

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
        }
        else
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection")!;
        }

        // Define scheduled jobs
        _scheduledJobs =
        [
            new ScheduledJob(
                Name: "StaleJobCleanup",
                CronExpression: "*/5 * * * *", // Every 5 minutes
                Handler: CleanupStaleJobsAsync
            ),
            new ScheduledJob(
                Name: "DeadLetterRetry",
                CronExpression: "0 */1 * * *", // Every hour
                Handler: RetryDeadLetterItemsAsync
            ),
            new ScheduledJob(
                Name: "ExpiredLockRelease",
                CronExpression: "*/2 * * * *", // Every 2 minutes
                Handler: ReleaseExpiredLocksAsync
            )
        ];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "CronScheduler started. WorkerId: {WorkerId}, Jobs: {JobCount}",
            _workerId, _scheduledJobs.Count);

        foreach (var job in _scheduledJobs)
        {
            _logger.LogInformation("  - {JobName}: {CronExpression}", job.Name, job.CronExpression);
        }

        // Run the scheduler loop
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;

            foreach (var job in _scheduledJobs)
            {
                if (ShouldRunJob(job, now))
                {
                    // Fire and forget - don't block other jobs
                    _ = TryExecuteJobAsync(job, stoppingToken);
                }
            }

            // Check every 30 seconds
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }

        _logger.LogInformation("CronScheduler stopped");
    }

    private bool ShouldRunJob(ScheduledJob job, DateTime now)
    {
        var cron = CronExpression.Parse(job.CronExpression);
        var lastRun = job.LastRunUtc ?? now.AddMinutes(-10); // Default to 10 min ago if never run

        var nextRun = cron.GetNextOccurrence(lastRun, TimeZoneInfo.Utc);
        return nextRun.HasValue && nextRun.Value <= now;
    }

    private async Task TryExecuteJobAsync(ScheduledJob job, CancellationToken cancellationToken)
    {
        var lockKey = $"cron-{job.Name}";
        var @lock = CreateLock(lockKey);

        // Try to acquire lock with short timeout - if another pod has it, skip
        await using var handle = await @lock.TryAcquireAsync(
            timeout: TimeSpan.FromSeconds(5),
            cancellationToken: cancellationToken);

        if (handle == null)
        {
            _logger.LogDebug(
                "Skipping job {JobName} - another instance holds the lock",
                job.Name);
            return;
        }

        _logger.LogInformation(
            "Acquired lock for job {JobName}, executing on {WorkerId}",
            job.Name, _workerId);

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await job.Handler(_serviceProvider, cancellationToken);
            stopwatch.Stop();

            job.LastRunUtc = DateTime.UtcNow;

            _logger.LogInformation(
                "Job {JobName} completed in {ElapsedMs}ms",
                job.Name, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job {JobName} failed", job.Name);
        }
    }

    private IDistributedLock CreateLock(string lockKey)
    {
        if (_options.DatabaseProvider == DatabaseProvider.Sqlite)
        {
            return new FileDistributedLock(_lockDirectory!, lockKey);
        }
        return new SqlDistributedLock(lockKey, _connectionString!);
    }

    // ============================================
    // Scheduled Job Handlers
    // ============================================

    /// <summary>
    /// Cleans up jobs that have been stuck in "Processing" status for too long.
    /// </summary>
    private async Task CleanupStaleJobsAsync(IServiceProvider services, CancellationToken ct)
    {
        using var scope = services.CreateScope();
        var dbFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BatchanatorDbContext>>();
        await using var db = await dbFactory.CreateDbContextAsync(ct);

        var staleThreshold = DateTime.UtcNow.AddHours(-24);

        // Find jobs stuck in Processing for over 24 hours
        var staleJobs = await db.Jobs
            .Where(j => j.Status == JobStatus.Processing && j.CreatedAt < staleThreshold)
            .ToListAsync(ct);

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
            await db.SaveChangesAsync(ct);
            _logger.LogInformation("Cleaned up {Count} stale jobs", staleJobs.Count);
        }
    }

    /// <summary>
    /// Optionally retries dead-letter items that may have failed due to transient issues.
    /// Only retries items that have been in dead-letter for at least 1 hour.
    /// </summary>
    private async Task RetryDeadLetterItemsAsync(IServiceProvider services, CancellationToken ct)
    {
        using var scope = services.CreateScope();
        var dbFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BatchanatorDbContext>>();
        await using var db = await dbFactory.CreateDbContextAsync(ct);

        var cooldownThreshold = DateTime.UtcNow.AddHours(-1);

        // Find dead-letter items that have been there for at least an hour
        // and haven't exceeded the extended retry limit (10 total attempts)
        var deadLetterItems = await db.BatchItems
            .Where(i => i.Status == BatchItemStatus.DeadLetter
                        && i.UpdatedAt < cooldownThreshold
                        && i.AttemptCount < 10) // Extended retry limit
            .Take(100) // Process in batches
            .ToListAsync(ct);

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
            await db.SaveChangesAsync(ct);
            _logger.LogInformation("Queued {Count} dead-letter items for retry", deadLetterItems.Count);
        }
    }

    /// <summary>
    /// Releases locks on items where the lock has expired but status is still Processing.
    /// This handles cases where a worker died without releasing its locks.
    /// </summary>
    private async Task ReleaseExpiredLocksAsync(IServiceProvider services, CancellationToken ct)
    {
        using var scope = services.CreateScope();
        var dbFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BatchanatorDbContext>>();
        await using var db = await dbFactory.CreateDbContextAsync(ct);

        var now = DateTime.UtcNow;

        // Find items with expired locks that are still in Processing status
        var expiredItems = await db.BatchItems
            .Where(i => i.Status == BatchItemStatus.Processing
                        && i.LockedUntil != null
                        && i.LockedUntil < now)
            .ToListAsync(ct);

        foreach (var item in expiredItems)
        {
            _logger.LogWarning(
                "Releasing expired lock on item {ItemId} (was locked by {LockedBy} until {LockedUntil})",
                item.Id, item.LockedBy, item.LockedUntil);

            item.Status = BatchItemStatus.Failed;
            item.LockedBy = null;
            item.LockedUntil = null;
            item.NextAttemptAt = now; // Immediately available for retry
        }

        if (expiredItems.Count > 0)
        {
            await db.SaveChangesAsync(ct);
            _logger.LogInformation("Released {Count} expired locks", expiredItems.Count);
        }
    }
}

/// <summary>
/// Represents a scheduled job with its cron expression and handler.
/// </summary>
internal record ScheduledJob(
    string Name,
    string CronExpression,
    Func<IServiceProvider, CancellationToken, Task> Handler)
{
    public DateTime? LastRunUtc { get; set; }
}
