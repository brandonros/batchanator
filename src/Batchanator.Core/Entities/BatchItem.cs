using Batchanator.Core.Enums;

namespace Batchanator.Core.Entities;

public class BatchItem
{
    public Guid Id { get; set; }
    public Guid BatchId { get; set; }
    public string SourceRowId { get; set; } = default!;
    public string IdempotencyKey { get; set; } = default!;
    public string PayloadJson { get; set; } = default!;
    public BatchItemStatus Status { get; set; }
    public int AttemptCount { get; set; }
    public string? LastError { get; set; }
    public string? ResultPayload { get; set; }
    public string? LockedBy { get; set; }
    public DateTime? LockedUntil { get; set; }
    public DateTime? NextAttemptAt { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }

    public Batch Batch { get; set; } = default!;
}
