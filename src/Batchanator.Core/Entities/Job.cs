using Batchanator.Core.Enums;

namespace Batchanator.Core.Entities;

public class Job
{
    public Guid Id { get; set; }
    public string Name { get; set; } = default!;
    public string JobType { get; set; } = default!;
    public JobStatus Status { get; set; }
    public int TotalBatches { get; set; }
    public int CompletedBatches { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? CompletedAt { get; set; }

    public ICollection<Batch> Batches { get; set; } = new List<Batch>();
}
