namespace Backend.Models;

public class KafkaTelemetryMessage
{
    public int DeviceId { get; set; }

    public double Value { get; set; }
}