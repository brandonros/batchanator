using Batchanator.Core;
using Medallion.Threading;
using Medallion.Threading.FileSystem;
using Medallion.Threading.SqlServer;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

/// <summary>
/// Factory for creating distributed locks. Abstracts the underlying lock provider
/// (SQL Server or FileSystem) based on configuration.
/// </summary>
public class DistributedLockFactory
{
    private readonly string? _connectionString;
    private readonly DirectoryInfo? _lockDirectory;
    private readonly DatabaseProvider _databaseProvider;

    public DistributedLockFactory(
        IOptions<BatchanatorOptions> options,
        IConfiguration configuration,
        ILogger<DistributedLockFactory> logger)
    {
        var opts = options.Value;
        _databaseProvider = opts.DatabaseProvider;

        if (_databaseProvider == DatabaseProvider.Sqlite)
        {
            var lockDir = string.IsNullOrWhiteSpace(opts.LockDirectory)
                ? Path.Combine(Path.GetTempPath(), "batchanator-locks")
                : opts.LockDirectory;
            _lockDirectory = new DirectoryInfo(lockDir);
            if (!_lockDirectory.Exists)
            {
                _lockDirectory.Create();
            }
            logger.LogInformation("Using FileSystem locks at {LockDirectory}", _lockDirectory.FullName);
        }
        else
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection")!;
        }
    }

    /// <summary>
    /// Creates a distributed lock with the specified key.
    /// </summary>
    public IDistributedLock Create(string lockKey)
    {
        if (_databaseProvider == DatabaseProvider.Sqlite)
        {
            return new FileDistributedLock(_lockDirectory!, lockKey);
        }
        return new SqlDistributedLock(lockKey, _connectionString!);
    }
}
