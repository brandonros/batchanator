namespace API.Models;

public record JobSummary(
    Guid Id,
    string Name,
    string JobType,
    string Status,
    int TotalBatches,
    int CompletedBatches,
    DateTime CreatedAt,
    DateTime? CompletedAt);

public record JobDetails(
    Guid Id,
    string Name,
    string JobType,
    string Status,
    int TotalBatches,
    int CompletedBatches,
    DateTime CreatedAt,
    DateTime? CompletedAt,
    int TotalItems,
    int SucceededItems,
    int FailedItems);

public record BatchSummary(
    Guid Id,
    int SequenceNumber,
    string Status,
    int TotalItems,
    int SucceededItems,
    int FailedItems,
    DateTime CreatedAt,
    DateTime? CompletedAt);

public record BatchItemSummary(
    Guid Id,
    string SourceRowId,
    string IdempotencyKey,
    string Status,
    int AttemptCount,
    string? LastError,
    DateTime CreatedAt,
    DateTime? UpdatedAt);
