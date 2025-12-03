using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

public class WorkClaimingService
{
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly BatchanatorOptions _options;
    private readonly ILogger<WorkClaimingService> _logger;
    private readonly string _workerId;

    public WorkClaimingService(
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<WorkClaimingService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _options = options.Value;
        _logger = logger;
        _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";
    }

    public async Task<List<WorkItem>> ClaimItemsAsync(CancellationToken cancellationToken)
    {
        if (_options.DatabaseProvider == DatabaseProvider.Sqlite)
        {
            return await ClaimItemsSqliteAsync(cancellationToken);
        }

        return await ClaimItemsSqlServerAsync(cancellationToken);
    }

    private async Task<List<WorkItem>> ClaimItemsSqliteAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batchSize = _options.ProcessingBatchSize;
        var lockTimeout = _options.LockTimeoutMinutes;
        var now = DateTime.UtcNow;
        var lockedUntil = now.AddMinutes(lockTimeout);

        // Atomic UPDATE-then-SELECT to prevent race conditions
        var claimToken = $"{_workerId}-{Guid.NewGuid():N}";

        var updateSql = @"
            UPDATE BatchItems
            SET Status = 'Processing',
                LockedBy = @claimToken,
                LockedUntil = @lockedUntil,
                UpdatedAt = @now
            WHERE Id IN (
                SELECT Id FROM BatchItems
                WHERE (Status = 'Pending' OR (Status = 'Failed' AND NextAttemptAt <= @now))
                  AND (LockedUntil IS NULL OR LockedUntil < @now)
                ORDER BY CreatedAt
                LIMIT @batchSize
            )";

        await dbContext.Database.ExecuteSqlRawAsync(
            updateSql,
            new Microsoft.Data.Sqlite.SqliteParameter("@claimToken", claimToken),
            new Microsoft.Data.Sqlite.SqliteParameter("@lockedUntil", lockedUntil),
            new Microsoft.Data.Sqlite.SqliteParameter("@now", now),
            new Microsoft.Data.Sqlite.SqliteParameter("@batchSize", batchSize));

        var items = await dbContext.BatchItems
            .Where(i => i.LockedBy == claimToken)
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        return await MapClaimedItemsToWorkItems(dbContext, items, cancellationToken);
    }

    private async Task<List<WorkItem>> ClaimItemsSqlServerAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var batchSize = _options.ProcessingBatchSize;
        var lockTimeout = _options.LockTimeoutMinutes;
        var now = DateTime.UtcNow;

        var sql = $@"
            WITH cte AS (
                SELECT TOP ({batchSize}) *
                FROM BatchItems WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE (Status = 'Pending' OR (Status = 'Failed' AND NextAttemptAt <= @now))
                  AND (LockedUntil IS NULL OR LockedUntil < @now)
                ORDER BY CreatedAt
            )
            UPDATE cte
            SET Status = 'Processing',
                LockedBy = @workerId,
                LockedUntil = DATEADD(MINUTE, {lockTimeout}, @now),
                UpdatedAt = @now
            OUTPUT inserted.*;
        ";

        var items = await dbContext.BatchItems
            .FromSqlRaw(sql,
                new Microsoft.Data.SqlClient.SqlParameter("@now", now),
                new Microsoft.Data.SqlClient.SqlParameter("@workerId", _workerId))
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        return await MapClaimedItemsToWorkItems(dbContext, items, cancellationToken);
    }

    private async Task<List<WorkItem>> MapClaimedItemsToWorkItems(
        BatchanatorDbContext dbContext,
        List<BatchItem> items,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
            return [];

        var batchIds = items.Select(i => i.BatchId).Distinct().ToList();
        var batchJobTypes = await dbContext.Batches
            .Where(b => batchIds.Contains(b.Id))
            .Include(b => b.Job)
            .ToDictionaryAsync(b => b.Id, b => b.Job.JobType, cancellationToken);

        return items.Select(item => new WorkItem(
            item.Id,
            item.BatchId,
            item.IdempotencyKey,
            item.PayloadJson,
            batchJobTypes.GetValueOrDefault(item.BatchId, "unknown")
        )).ToList();
    }
}

public record WorkItem(Guid ItemId, Guid BatchId, string IdempotencyKey, string PayloadJson, string JobType);
