using System.Net.Http.Json;
using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Dtos;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Scheduler.Services;

public class DispatcherHostedService : BackgroundService
{
    private readonly WorkClaimingService _claimingService;
    private readonly ReconciliationService _reconciliationService;
    private readonly IDbContextFactory<BatchanatorDbContext> _dbContextFactory;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IOptions<BatchanatorOptions> _options;
    private readonly ILogger<DispatcherHostedService> _logger;
    private readonly string _workerId;

    public DispatcherHostedService(
        WorkClaimingService claimingService,
        ReconciliationService reconciliationService,
        IDbContextFactory<BatchanatorDbContext> dbContextFactory,
        IHttpClientFactory httpClientFactory,
        IOptions<BatchanatorOptions> options,
        ILogger<DispatcherHostedService> logger)
    {
        _claimingService = claimingService;
        _reconciliationService = reconciliationService;
        _dbContextFactory = dbContextFactory;
        _httpClientFactory = httpClientFactory;
        _options = options;
        _logger = logger;
        _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Dispatcher started. WorkerId: {WorkerId}", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processedCount = await ProcessBatchAsync(stoppingToken);

                if (processedCount == 0)
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(_options.Value.PollingIntervalSeconds),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in dispatcher loop");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        _logger.LogInformation("Dispatcher stopped");
    }

    private async Task<int> ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var workItems = await _claimingService.ClaimItemsAsync(cancellationToken);

        if (workItems.Count == 0)
        {
            return 0;
        }

        _logger.LogInformation("Claimed {Count} items for processing", workItems.Count);

        var maxConcurrency = _options.Value.MaxConcurrencyPerWorker;
        using var throttler = new SemaphoreSlim(maxConcurrency);

        var tasks = workItems.Select(async item =>
        {
            await throttler.WaitAsync(cancellationToken);
            try
            {
                await ProcessItemAsync(item, cancellationToken);
            }
            finally
            {
                throttler.Release();
            }
        });

        await Task.WhenAll(tasks);

        return workItems.Count;
    }

    private async Task ProcessItemAsync(WorkItem workItem, CancellationToken cancellationToken)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("BatchanatorApi");
            var request = new ProcessRequest(workItem.JobType, workItem.IdempotencyKey, workItem.PayloadJson);
            var response = await client.PostAsJsonAsync("/process", request, cancellationToken);
            var result = await response.Content.ReadFromJsonAsync<ProcessResponse>(cancellationToken);

            await UpdateItemResultAsync(workItem.ItemId, workItem.BatchId, result!, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing item {ItemId}", workItem.ItemId);

            var errorResult = new ProcessResponse(false, ex.Message, null);
            await UpdateItemResultAsync(workItem.ItemId, workItem.BatchId, errorResult, cancellationToken);
        }
    }

    private async Task UpdateItemResultAsync(Guid itemId, Guid batchId, ProcessResponse result, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var item = await dbContext.BatchItems.FindAsync([itemId], cancellationToken);
        if (item == null) return;

        var now = DateTime.UtcNow;
        var options = _options.Value;

        if (result.Success)
        {
            item.Status = BatchItemStatus.Succeeded;
            item.ResultPayload = result.Result;
            item.LastError = null;
        }
        else
        {
            item.AttemptCount++;
            item.LastError = result.Error;

            if (item.AttemptCount >= options.MaxAttempts)
            {
                item.Status = BatchItemStatus.DeadLetter;
            }
            else
            {
                item.Status = BatchItemStatus.Failed;
                var backoffSeconds = Math.Pow(2, item.AttemptCount);
                item.NextAttemptAt = now.AddSeconds(backoffSeconds);
            }
        }

        item.LockedBy = null;
        item.LockedUntil = null;
        item.UpdatedAt = now;

        await dbContext.SaveChangesAsync(cancellationToken);

        await _reconciliationService.ReconcileBatchAsync(batchId, cancellationToken);
    }
}
