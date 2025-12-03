namespace Batchanator.Core;

public enum DatabaseProvider
{
    SqlServer,
    Sqlite
}

public class BatchanatorOptions
{
    public const string SectionName = "Batchanator";

    public int ChunkSize { get; set; } = 1000;
    public int MaxAttempts { get; set; } = 5;
    public int LockTimeoutMinutes { get; set; } = 5;
    public int PollingIntervalSeconds { get; set; } = 5;
    public int ProcessingBatchSize { get; set; } = 50;
    public string ApiBaseUrl { get; set; } = "http://localhost:5000";

    /// <summary>
    /// Maximum number of concurrent API calls per worker pod.
    /// Controls backpressure to downstream services.
    /// </summary>
    public int MaxConcurrencyPerWorker { get; set; } = 10;

    /// <summary>
    /// The database provider to use. SqlServer for production, Sqlite for local development.
    /// </summary>
    public DatabaseProvider DatabaseProvider { get; set; } = DatabaseProvider.SqlServer;

    /// <summary>
    /// Directory path for FileSystem-based locks when using Sqlite.
    /// Defaults to system temp directory.
    /// </summary>
    public string LockDirectory { get; set; } = Path.Combine(Path.GetTempPath(), "batchanator-locks");

    /// <summary>
    /// Path to the SQLite database file when using Sqlite provider.
    /// Defaults to batchanator.db in current directory.
    /// </summary>
    public string SqlitePath { get; set; } = "batchanator.db";

    /// <summary>
    /// Scheduled database ingestion jobs. Each entry defines a job type and its cron schedule.
    /// </summary>
    public List<ScheduledIngestionJob> ScheduledIngestionJobs { get; set; } = [];

    /// <summary>
    /// Maintenance job schedules. Override defaults if needed.
    /// </summary>
    public MaintenanceSchedules Maintenance { get; set; } = new();
}

/// <summary>
/// Configuration for a scheduled database ingestion job.
/// </summary>
public class ScheduledIngestionJob
{
    /// <summary>
    /// The job type to ingest (must match a registered handler).
    /// </summary>
    public string JobType { get; set; } = default!;

    /// <summary>
    /// Cron expression for the schedule (e.g., "0 * * * *" for hourly).
    /// </summary>
    public string CronExpression { get; set; } = "0 * * * *";

    /// <summary>
    /// Whether this job is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;
}

/// <summary>
/// Cron schedules for built-in maintenance jobs.
/// </summary>
public class MaintenanceSchedules
{
    /// <summary>
    /// Cleanup jobs stuck in Processing for over 24 hours.
    /// Default: every 5 minutes.
    /// </summary>
    public string StaleJobCleanup { get; set; } = "*/5 * * * *";

    /// <summary>
    /// Retry dead-letter items that have cooled down.
    /// Default: every hour.
    /// </summary>
    public string DeadLetterRetry { get; set; } = "0 * * * *";

    /// <summary>
    /// Release locks on items where the lock expired but status is still Processing.
    /// Default: every 2 minutes.
    /// </summary>
    public string ExpiredLockRelease { get; set; } = "*/2 * * * *";
}
