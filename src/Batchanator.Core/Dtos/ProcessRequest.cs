namespace Batchanator.Core.Dtos;

public record ProcessRequest(string JobType, string IdempotencyKey, string Payload);
