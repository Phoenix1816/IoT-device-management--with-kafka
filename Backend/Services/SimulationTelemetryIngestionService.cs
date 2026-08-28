using System.Text.Json;
using Backend.Services.Interfaces;

namespace Backend.Services;

public class SimulationTelemetryIngestionService
    : ITelemetryIngestionService
{
    public Task StartAsync(CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    public Task<RawTelemetryDto> ProcessIncomingAsync(
        string rawPayload,
        string deviceId)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            throw new ArgumentException(
                "Device ID cannot be empty",
                nameof(deviceId));
        }

        if (string.IsNullOrWhiteSpace(rawPayload))
        {
            throw new ArgumentException(
                "Telemetry payload cannot be empty",
                nameof(rawPayload));
        }

        using var document =
            JsonDocument.Parse(rawPayload);

        if (!document.RootElement.TryGetProperty(
                "value",
                out var valueProperty))
        {
            throw new InvalidOperationException(
                "Telemetry payload does not contain a 'value' property.");
        }

        var value =
            valueProperty.GetDouble();

        var telemetry =
            new RawTelemetryDto(
                deviceId,
                value);

        return Task.FromResult(telemetry);
    }
}