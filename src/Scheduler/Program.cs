using Batchanator.Core;
using Batchanator.Core.Data;
using Microsoft.EntityFrameworkCore;
using Scheduler.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration
builder.Services.Configure<BatchanatorOptions>(
    builder.Configuration.GetSection(BatchanatorOptions.SectionName));

var batchanatorConfig = builder.Configuration.GetSection(BatchanatorOptions.SectionName).Get<BatchanatorOptions>() ?? new BatchanatorOptions();

// Database - conditional Sqlite or SQL Server
if (batchanatorConfig.DatabaseProvider == DatabaseProvider.Sqlite)
{
    var sqlitePath = batchanatorConfig.SqlitePath;
    builder.Services.AddDbContextFactory<BatchanatorDbContext>(options =>
        options.UseSqlite($"Data Source={sqlitePath}"));
}
else
{
    var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
    builder.Services.AddDbContextFactory<BatchanatorDbContext>(options =>
        options.UseSqlServer(connectionString));
}

// HTTP Client for API calls
var apiBaseUrl = builder.Configuration.GetSection("Batchanator:ApiBaseUrl").Value ?? "http://localhost:5000";
builder.Services.AddHttpClient("BatchanatorApi", client =>
{
    client.BaseAddress = new Uri(apiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(30);
});

// Ingestion services
builder.Services.AddScoped<BatchIngestionService>();
builder.Services.AddScoped<FileIngestionService>();
builder.Services.AddScoped<DatabaseIngestionService>();

// Background services
builder.Services.AddHostedService<DispatcherHostedService>();
builder.Services.AddHostedService<CronSchedulerService>();

// Controllers
builder.Services.AddControllers();
builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
