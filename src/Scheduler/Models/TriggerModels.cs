namespace Scheduler.Models;

// Response (shared)
public record TriggerResponse(Guid JobId, string JobType, string Status);

// File trigger
public record FileTriggerRequest(string FilePath);

// Items trigger - submit work items directly
public record ItemsTriggerRequest(string JobName, List<WorkItemInput> Items);
public record WorkItemInput(string IdempotencyKey, string PayloadJson);
