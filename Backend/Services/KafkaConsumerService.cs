using System.Text.Json;
using Backend.Data;
using Backend.Hubs;
using Backend.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;

namespace Backend.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IHubContext<TelemetryHub> _hubContext;

    private IConsumer<Ignore, string>? _consumer;

    private const string TopicName = "telemetry";

    public KafkaConsumerService(
        IConfiguration configuration,
        ILogger<KafkaConsumerService> logger,
        IServiceScopeFactory scopeFactory,
        IHubContext<TelemetryHub> hubContext)
    {
        _configuration = configuration;
        _logger = logger;
        _scopeFactory = scopeFactory;
        _hubContext = hubContext;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var bootstrapServers =
            _configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,

            GroupId = "iot-telemetry-consumer",

            AutoOffsetReset =
                AutoOffsetReset.Earliest,

            EnableAutoCommit = true
        };

        _consumer =
            new ConsumerBuilder<Ignore, string>(config)
                .Build();

        _consumer.Subscribe(TopicName);

        _logger.LogInformation(
            "Kafka Consumer started. Topic: {Topic}",
            TopicName);

        // Kafka Consume() çağrısının ASP.NET Core
        // startup sürecini bloklamasını engelle.
        await Task.Yield();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result =
                        _consumer.Consume(stoppingToken);

                    var rawMessage =
                        result.Message.Value;

                    _logger.LogInformation(
                        "Kafka message received: {Message}",
                        rawMessage);

                    // JSON PARSE
                    var telemetry =
                        JsonSerializer.Deserialize<KafkaTelemetryMessage>(
                            rawMessage,
                            new JsonSerializerOptions
                            {
                                PropertyNameCaseInsensitive = true
                            });

                    if (telemetry == null)
                    {
                        _logger.LogWarning(
                            "Kafka message could not be deserialized.");

                        continue;
                    }

                    if (telemetry.DeviceId <= 0)
                    {
                        _logger.LogWarning(
                            "Kafka message has invalid DeviceId: {DeviceId}",
                            telemetry.DeviceId);

                        continue;
                    }

                    // DATABASE SCOPE
                    using var scope =
                        _scopeFactory.CreateScope();

                    var context =
                        scope.ServiceProvider
                            .GetRequiredService<AppDbContext>();

                    // DEVICE BUL
                    var device =
                        await context.Devices
                            .FirstOrDefaultAsync(
                                d =>
                                    d.Id == telemetry.DeviceId &&
                                    d.IsActive,
                                stoppingToken);

                    if (device == null)
                    {
                        _logger.LogWarning(
                            "Active device not found: {DeviceId}",
                            telemetry.DeviceId);

                        continue;
                    }

                    // TELEMETRY OLUŞTUR
                    var telemetryLog =
                        new TelemetryLog
                        {
                            DeviceId = device.Id,

                            Metric = "Temperature",

                            Value = telemetry.Value,

                            Unit = "°C",

                            Timestamp = DateTime.UtcNow
                        };

                    context.TelemetryLogs.Add(
                        telemetryLog);

                    // LAST SEEN
                    device.LastSeen =
                        telemetryLog.Timestamp;

                    // DATABASE SAVE
                    await context.SaveChangesAsync(
                        stoppingToken);

                    // SIGNALR
                    await _hubContext.Clients.All.SendAsync(
                        "ReceiveTelemetry",
                        new
                        {
                            DeviceId = device.Id,

                            Metric =
                                telemetryLog.Metric,

                            Value =
                                telemetryLog.Value,

                            Unit =
                                telemetryLog.Unit,

                            Timestamp =
                                telemetryLog.Timestamp,

                            Threshold =
                                device.Threshold,

                            LastSeen =
                                device.LastSeen
                        },
                        stoppingToken);

                    _logger.LogInformation(
                        "Telemetry processed. " +
                        "Device: {DeviceId}, " +
                        "Value: {Value}",
                        device.Name,
                        telemetry.Value);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(
                        ex,
                        "Kafka consume error.");
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "Invalid Kafka telemetry JSON.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Unexpected error while processing Kafka telemetry.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Kafka Consumer stopping.");
        }
        finally
        {
            _consumer?.Close();
            _consumer?.Dispose();
            _consumer = null;
        }

        await Task.CompletedTask;
    }
}