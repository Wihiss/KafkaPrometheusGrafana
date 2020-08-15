using Confluent.Kafka;
using My.Rtm;
using System;
using System.Collections.Generic;
using System.Threading;
using TestKafkaLib;

namespace TestKafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

            Action<DeliveryReport<string, BboMsg>> deliveryHandler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message '{r.Message.Value.ToDetailedString()}' to {r.TopicPartitionOffset}"
                        : $"Delivery Error: {r.Error.Reason}");            

            Dictionary<string, int> secNumbers = new Dictionary<string, int>();

            using (var p = new ProducerBuilder<string, BboMsg>(conf).SetValueSerializer(new ProtoSerializer<BboMsg>()).Build())
            {                
                int symNumber = 10;
                Random rnd = new Random();

                byte priceUpperBound = 10;
                byte priceLowerBound = 12;

                int secNum = 0;

                TopicPartition tp1 = new TopicPartition("testTopic", new Partition(0));
                TopicPartition tp2 = new TopicPartition("testTopic", new Partition(1));

                while (true)
                {
                    int symbolIndex = rnd.Next(1, 10);

                    TopicPartition tp = (symbolIndex % 2 == 0) ? tp1 : tp2;

                    string symbol = "Symbol_" + symbolIndex;

                    double prc = rnd.NextDouble() * (priceUpperBound - priceLowerBound) + priceLowerBound;
                    double qty = rnd.Next(10, 20);
                    Side side = rnd.Next(0, 1) == 0 ? Side.Ask : Side.Bid;

                    ++secNum;

                    //p.Produce(tp, new Message<string, BboMsg>
                    p.Produce("testTopic", new Message<string, BboMsg>
                    {
                        Key = symbol,
                        Value = new BboMsg()
                        {
                            Prc = prc,
                            Qty = qty,
                            Side = side,
                            Symbol = symbol,
                            SeqNumber = secNum
                        }
                    }, deliveryHandler);

                    Thread.Sleep(500);
                }

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
