namespace Backend.Services.Interfaces;

public interface IKafkaProducerService
{
    Task PublishAsync(
        int deviceId,
        double value,
        CancellationToken cancellationToken = default);
}