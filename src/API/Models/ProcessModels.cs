namespace API.Models;

public record ProcessRequest(string JobType, string IdempotencyKey, string Payload);
public record ProcessResponse(bool Success, string? Error, string? Result);
public record HandlerInfo(string JobType, string PayloadTypeName);
