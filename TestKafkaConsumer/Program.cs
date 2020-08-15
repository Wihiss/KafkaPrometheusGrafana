using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using My.Rtm;
using TestKafkaLib;

namespace TestKafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                // AutoOffsetReset = AutoOffsetReset.Earliest
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                ClientId = Guid.NewGuid().ToString()
            };

            int consNum = Int32.Parse(args[0]);
            if (consNum < 1)
            {
                Console.WriteLine("1th argument must be 'consumer number' (>0).");
            }
            else
            {
                for (int i = 0; i < consNum; ++i)
                {
                    int consumerIndex = i;

                    Task.Run( () => StartConsumer(new Tuple<ConsumerConfig, int>(conf, consumerIndex)));
                }
            }

            Thread.Sleep(20000);
        }

        private static void StartConsumer(object data)
        {
            Tuple<ConsumerConfig, int> prm = (Tuple<ConsumerConfig, int>) data;

            using (var c = new ConsumerBuilder<Ignore, BboMsg>(prm.Item1).SetValueDeserializer(new ProtoDeserializer<BboMsg>()).Build())
            {
                c.Subscribe("testTopic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                Console.WriteLine("Consumer " + prm.Item2 + " is started and waiting for new messages...");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value.ToDetailedString()}' from consumer {prm.Item2}: '{cr.Topic} / {cr.TopicPartition.Partition.Value}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
