namespace Batchanator.Core.Dtos;

public record ProcessResponse(bool Success, string? Error, string? Result);
