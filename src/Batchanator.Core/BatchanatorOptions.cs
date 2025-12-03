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
}
