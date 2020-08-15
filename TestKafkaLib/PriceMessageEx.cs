using My.Rtm;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKafkaLib
{
    public static class PriceMessageEx
    {
        public static string ToDetailedString(this BboMsg m)
        {
            return $"SeqNum: {m.SeqNumber}, Symbol: {m.Symbol}, Side: {m.Side}, Qty: {m.Qty}, Prc: {m.Prc}";
        }
    }
}
