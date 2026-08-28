using Backend.Data;
using Backend.Services;
using Backend.Services.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Backend.BackgroundServices;

public class TelemetrySimulationWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<TelemetrySimulationWorker> _logger;
    private readonly OpenMeteoService _openMeteoService;
    private readonly IKafkaProducerService _kafkaProducerService;

    public TelemetrySimulationWorker(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<TelemetrySimulationWorker> logger,
        OpenMeteoService openMeteoService,
        IKafkaProducerService kafkaProducerService)
    {
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _logger = logger;
        _openMeteoService = openMeteoService;
        _kafkaProducerService = kafkaProducerService;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var intervalMs =
            _configuration.GetValue<int>(
                "Simulation:IntervalMs");

        const double latitude = 41.0082;
        const double longitude = 28.9784;

        _logger.LogInformation(
            "Telemetry worker started. " +
            "Real data source: Open-Meteo. " +
            "Kafka pipeline enabled. " +
            "Interval: {Interval}ms",
            intervalMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // ==========================================
                // OPEN-METEO
                // ==========================================

                var weather =
                    await _openMeteoService
                        .GetCurrentWeatherAsync(
                            latitude,
                            longitude,
                            stoppingToken);

                if (weather?.Current == null)
                {
                    _logger.LogWarning(
                        "Open-Meteo'dan telemetry verisi alınamadı.");

                    await Task.Delay(
                        1000,
                        stoppingToken);

                    continue;
                }

                var temperature =
                    weather.Current.Temperature_2m;

                _logger.LogInformation(
                    "Open-Meteo temperature: {Temperature} °C",
                    temperature);


                // ==========================================
                // DATABASE SCOPE
                // ==========================================

                using var scope =
                    _scopeFactory.CreateScope();

                var context =
                    scope.ServiceProvider
                        .GetRequiredService<AppDbContext>();


                // ==========================================
                // AKTİF CİHAZLARI AL
                // ==========================================

                var devices =
                    await context.Devices
                        .Where(d => d.IsActive)
                        .Select(d => d.Id)
                        .ToListAsync(
                            stoppingToken);


                // ==========================================
                // KAFKA'YA GÖNDER
                // ==========================================

                foreach (var deviceId in devices)
                {
                    await _kafkaProducerService
                        .PublishAsync(
                            deviceId,
                            temperature,
                            stoppingToken);

                    _logger.LogInformation(
                        "Telemetry published to Kafka. " +
                        "Device: {DeviceId}, " +
                        "Value: {Value}",
                        deviceId,
                        temperature);
                }


                // ==========================================
                // NEXT ITERATION
                // ==========================================

                await Task.Delay(
                    intervalMs,
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Telemetry üretimi sırasında hata oluştu.");

                await Task.Delay(
                    5000,
                    stoppingToken);
            }
        }

        _logger.LogInformation(
            "Telemetry worker stopped.");
    }
}