using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace TestKafkaProducer
{
    public class ProtoSerializer<T> : ISerializer<T> where T : Google.Protobuf.IMessage<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return data.ToByteArray();
        }
    }
}
