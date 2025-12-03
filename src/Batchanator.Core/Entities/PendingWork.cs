namespace Batchanator.Core.Entities;

public class PendingWork
{
    public Guid Id { get; set; }
    public string JobType { get; set; } = string.Empty;
    public string IdempotencyKey { get; set; } = string.Empty;
    public string PayloadJson { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}
