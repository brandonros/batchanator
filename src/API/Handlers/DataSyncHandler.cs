using System.Text.Json;
using Batchanator.Core;

namespace API.Handlers;

public class DataSyncPayload
{
    public string ExternalSystemId { get; set; } = default!;
    public string EntityType { get; set; } = default!;
    public string EntityId { get; set; } = default!;
    public string Operation { get; set; } = default!; // "create", "update", "delete"
    public JsonElement? Data { get; set; }
}

public class DataSyncHandler : BatchJobHandlerBase<DataSyncPayload>
{
    private readonly ILogger<DataSyncHandler> _logger;

    public DataSyncHandler(ILogger<DataSyncHandler> logger)
    {
        _logger = logger;
    }

    public override string JobType => "data-sync";

    public override async Task<BatchJobResult> ProcessAsync(
        DataSyncPayload payload,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Syncing {EntityType}/{EntityId} ({Operation}) to {ExternalSystem} with key {IdempotencyKey}",
            payload.EntityType, payload.EntityId, payload.Operation, payload.ExternalSystemId, idempotencyKey);

        // Validate operation
        if (payload.Operation is not ("create" or "update" or "delete"))
        {
            return BatchJobResult.Failed($"Invalid operation: {payload.Operation}. Must be create, update, or delete.");
        }

        // Simulate API call to external system (200-800ms)
        await Task.Delay(Random.Shared.Next(200, 800), cancellationToken);

        // Simulate various failure scenarios (10% failure rate)
        var failureChance = Random.Shared.NextDouble();
        if (failureChance < 0.05)
        {
            return BatchJobResult.Failed("External system rate limit exceeded");
        }
        if (failureChance < 0.10)
        {
            return BatchJobResult.Failed("External system returned 503 Service Unavailable");
        }

        var result = new
        {
            syncedAt = DateTime.UtcNow,
            externalSystemId = payload.ExternalSystemId,
            externalRecordId = $"ext_{payload.EntityId}_{DateTime.UtcNow.Ticks}",
            operation = payload.Operation
        };

        return BatchJobResult.Succeeded(JsonSerializer.Serialize(result));
    }
}
