using Confluent.Kafka;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKafkaConsumer
{
    public class ProtoDeserializer<T> : IDeserializer<T> where T : Google.Protobuf.IMessage<T>, new()
    {
        private MessageParser<T> _parser;

        public ProtoDeserializer() { _parser = new MessageParser<T>(() => new T()); }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return default(T);

            return _parser.ParseFrom(data.ToArray());
        }
    }
}
