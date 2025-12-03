# batchanator

A lightweight .NET batch-processing framework

## Components
- **Core** — job/batch model, EF entities, handler abstraction  
- **API** — `/process` endpoint + pluggable job handlers  
- **Scheduler** — file ingestion, worker dispatcher, cron jobs

## Usage

```shell
dotnet run --project src/API --urls http://localhost:5000
```

```shell
dotnet run --project src/Scheduler --urls http://localhost:5001
```

```shell
curl -X POST http://localhost:5001/jobs/email-notification/trigger \
  -H "Content-Type: application/json" \
  -d '{"filePath": "/Users/brandon/Desktop/batchanator/samples/email-notifications.ndjson"}'
```
