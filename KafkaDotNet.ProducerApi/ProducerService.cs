using Confluent.Kafka;

namespace KafkaDotNet.ProducerApi
{
    public class ProducerService
    {
        private readonly ILogger<ProducerService> _logger;
        public ProducerService(ILogger<ProducerService> logger)
        {
            _logger = logger;
        }

        public async Task ProduceAsync(CancellationToken token)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                AllowAutoCreateTopics = true,
                Acks = Acks.All
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                var result = await producer.ProduceAsync(topic: "test-topic", 
                    new Message<Null, string> 
                    { 
                        Value = $"{DateTime.UtcNow} - Hello World!" 
                    }, 
                    token);
                _logger.LogInformation($"Delivered message!\nValue: {result.Value}\nOffset: {result.TopicPartitionOffset}");
            }
            catch (ProduceException<Null, string> e)
            {
                _logger.LogError($"Delivery failed: {e.Error.Reason}");
            }

            producer.Flush(token);
            producer.Dispose();
        }
    }
}
