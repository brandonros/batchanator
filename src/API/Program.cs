using API.Handlers;
using Batchanator.Core;
using Batchanator.Core.Data;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var batchanatorConfig = builder.Configuration
    .GetSection(BatchanatorOptions.SectionName)
    .Get<BatchanatorOptions>() ?? new BatchanatorOptions();

// Database
if (batchanatorConfig.DatabaseProvider == DatabaseProvider.Sqlite)
{
    builder.Services.AddDbContext<BatchanatorDbContext>(options =>
        options.UseSqlite($"Data Source={batchanatorConfig.SqlitePath}"));
}
else
{
    var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
    builder.Services.AddDbContext<BatchanatorDbContext>(options =>
        options.UseSqlServer(connectionString, sql =>
            sql.EnableRetryOnFailure(
                maxRetryCount: 5,
                maxRetryDelay: TimeSpan.FromSeconds(30),
                errorNumbersToAdd: null)));
}

// Job handlers
builder.Services.AddScoped<IBatchJobHandler, EmailNotificationHandler>();
builder.Services.AddScoped<IBatchJobHandler, DataSyncHandler>();
builder.Services.AddScoped<IBatchJobHandler, ReportGenerationHandler>();

// Controllers
builder.Services.AddControllers();
builder.Services.AddOpenApi();

var app = builder.Build();

// Apply migrations on startup
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<BatchanatorDbContext>();
    db.Database.Migrate();
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
