using Batchanator.Core.Enums;

namespace Batchanator.Core.Entities;

public class Batch
{
    public Guid Id { get; set; }
    public Guid JobId { get; set; }
    public int SequenceNumber { get; set; }
    public BatchStatus Status { get; set; }
    public int TotalItems { get; set; }
    public int SucceededItems { get; set; }
    public int FailedItems { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? CompletedAt { get; set; }

    public Job Job { get; set; } = default!;
    public ICollection<BatchItem> Items { get; set; } = new List<BatchItem>();
}
