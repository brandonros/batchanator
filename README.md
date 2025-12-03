# Batchanator

A .NET batch-processing framework with idempotent execution, distributed locking, and automatic retries.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Ingestion     │     │    Scheduler    │     │      API        │
│                 │     │                 │     │                 │
│  • File         │────▶│  • Dispatcher   │────▶│  • /process     │
│  • Database     │     │  • Cron jobs    │     │  • Job handlers │
│  • Direct API   │     │  • Lock release │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                      │                       │
         └──────────────────────┴───────────────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │   SQL Server / SQLite   │
                    │                         │
                    │  Jobs → Batches → Items │
                    └─────────────────────────┘
```

## Key Features

| Feature | Implementation |
|---------|----------------|
| **Idempotency** | Unique constraint on `IdempotencyKey`. Duplicate submissions are detected and skipped. |
| **Distributed locking** | Medallion.Threading (SQL Server locks or file-based for SQLite). Prevents double-processing across pods. |
| **Automatic retries** | Exponential backoff (2^attempt seconds). Configurable max attempts before dead-letter. |
| **Chunked ingestion** | Large files/datasets split into batches. Configurable chunk size. |
| **Worker claiming** | Row-level locking with `UPDLOCK, READPAST` (SQL Server) or optimistic locking (SQLite). |
| **Stale lock recovery** | Cron job releases expired locks every 2 minutes. |
| **Transient fault handling** | Polly retry for HTTP (3 retries, exponential backoff). EF Core retry for SQL Server transient errors. |
| **Schema management** | EF Core migrations. Database created/updated automatically on startup. |

## Ingestion Methods

```bash
# From file (NDJSON)
POST /jobs/{jobType}/trigger/file
{"filePath": "/path/to/data.ndjson"}

# From database (PendingWork table)
POST /jobs/{jobType}/trigger/database

# Direct submission
POST /jobs/{jobType}/trigger/items
{"jobName": "my-batch", "items": [{"idempotencyKey": "abc", "payloadJson": "{}"}]}
```

## Quick Start

```bash
# Terminal 1: API
dotnet run --project src/API --urls http://localhost:5000

# Terminal 2: Scheduler
dotnet run --project src/Scheduler --urls http://localhost:5001

# Trigger a job
curl -X POST http://localhost:5001/jobs/email-notification/trigger/file \
  -H "Content-Type: application/json" \
  -d '{"filePath": "samples/email-notifications.ndjson"}'
```

## Configuration

```json
{
  "Batchanator": {
    "DatabaseProvider": "Sqlite",
    "SqlitePath": "batchanator.db",
    "ChunkSize": 1000,
    "MaxAttempts": 5,
    "MaxConcurrencyPerWorker": 10,
    "PollingIntervalSeconds": 5,
    "LockTimeoutMinutes": 5
  }
}
```

## Job Handlers

Implement `IBatchJobHandler` to add custom job types:

```csharp
public class MyHandler : BatchJobHandlerBase<MyPayload>
{
    public override string JobType => "my-job";

    protected override async Task<BatchJobResult> ProcessAsync(
        MyPayload payload,
        string idempotencyKey,
        CancellationToken ct)
    {
        // Your logic here
        return BatchJobResult.Success();
    }
}
```

## Project Structure

```
src/
├── Batchanator.Core/          # Entities, DbContext, handler interface
├── API/                       # Process endpoint, job handlers
│   ├── Controllers/
│   ├── Handlers/
│   └── Models/
└── Scheduler/                 # Ingestion, dispatcher, cron
    ├── Controllers/
    ├── Services/
    └── Models/
```

## Status Endpoints (API)

```
GET  /jobs                     # List jobs
GET  /jobs/{id}                # Job details
GET  /jobs/{id}/batches        # Batches for job
GET  /batches/{id}/items       # Items in batch
GET  /handlers                 # Registered handlers
```

## Database Schema

- **Jobs** — Top-level unit of work (e.g., one file ingestion)
- **Batches** — Chunks within a job (configurable size)
- **BatchItems** — Individual work items with status tracking
- **PendingWork** — Staging table for database-triggered ingestion
