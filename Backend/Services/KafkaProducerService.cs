using System.Text.Json;
using Backend.Services.Interfaces;
using Confluent.Kafka;

namespace Backend.Services;

public class KafkaProducerService : IKafkaProducerService
{
    private readonly IProducer<Null, string> _producer;

    private const string TopicName = "telemetry";

    public KafkaProducerService(
        IConfiguration configuration)
    {
        var bootstrapServers =
            configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        _producer =
            new ProducerBuilder<Null, string>(config)
                .Build();
    }

    public async Task PublishAsync(
        int deviceId,
        double value,
        CancellationToken cancellationToken = default)
    {
        var message = new
        {
            deviceId,
            value
        };

        var json =
            JsonSerializer.Serialize(message);

        await _producer.ProduceAsync(
            TopicName,
            new Message<Null, string>
            {
                Value = json
            },
            cancellationToken);
    }
}