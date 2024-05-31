using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using Aspire.Core;
using System.IO;
using System;

namespace ConsoleApplication2
{
    class Program
    {
        public static string sNatsStream = "ASPIREWEBUI";
        public static IConnection natsCon;
        public static ConsumerConfiguration natsConsumerConfig;
        public static PushSubscribeOptions natsPushSubscribeOpts;
        public static IJetStreamPushSyncSubscription subscriberHandle;
        private static IJetStream natsJetStream;
        private static IJetStreamManagement natsJsManager;
        private static StreamConfiguration natsStreamConfig;
        private static IJetStreamPushSyncSubscription[] natsUISubsribersList;

        void LoadConsumerconfig()
        {
            natsConsumerConfig = ConsumerConfiguration.Builder()
                          .WithAckPolicy(AckPolicy.Explicit)
                          .WithDurable(sNatsStream + "Durable")
                          .WithDeliverGroup(sNatsStream + "Group")
                          .WithMaxAckPending(200)
                          .WithAckWait(60000)
                          .Build();

            natsPushSubscribeOpts = PushSubscribeOptions.Builder()
                .WithStream(sNatsStream)
                .WithConfiguration(natsConsumerConfig)
                .Build();
        }

        public static bool IsNatsConnected()
        {
            if ((natsCon != null) && (natsCon.State == ConnState.CONNECTED))
                return true;
            return false;
        }
        static void Main(string[] args)
        {
            string natsUrl = null;
            if (natsUrl == null)
            {
                natsUrl = "nats://172.16.11.71:4222";
            }


            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = natsUrl;
            opts.User = "nouman";
            opts.Password = "nouman.nawaz@avanzasolutions.com";


            ConnectionFactory cf = new ConnectionFactory();
            natsCon = cf.CreateConnection(opts);

            //if (IsNatsConnected())
            //{
            //    subscriberHandle = natsJetStream.PushSubscribeSync(sNatsStream + ".>", sNatsStream + "Group", natsPushSubscribeOpts);
            //    if (subscriberHandle != null)
            //    {
            //      Console.WriteLine("Nats stream [" + sNatsStream + "] subscription successful on NATS server.");
            //    }
            //    else
            //    {
            //        //natsServerConnected = false;
            //        Console.WriteLine("Nats stream [" + sNatsStream + "] subscription unsuccessful on NATS server.");
            //    }
            //}

            UIQueueMessage message = new UIQueueMessage("RefreshParticipantAccount");

            var itm = Newtonsoft.Json.JsonConvert.SerializeObject(message);

            byte[] payload = Encoding.ASCII.GetBytes(itm);

            MsgHeader msgHeader = new MsgHeader();
            msgHeader.Add("WriterSource", "userinterface");
            Msg natsMessage = new Msg("ASPIREWEBUI.FREE_REQUESTS", msgHeader, payload); // Nouman - Message segregation
           natsCon.Publish(natsMessage); // Nouman - Message segregation
            ByteArrayToFile("file.txt", payload);
            natsCon.Publish("ASPIREWEBUI.ASPIREWEBUI.", Encoding.UTF8.GetBytes("hello joe 1"));



            //try
            //{
            //    Msg m = subsribe.NextMessage(1000);
            //    string text = Encoding.UTF8.GetString(m.Data);
            //    Console.WriteLine($"Sync subscription received the message '{text}' from subject '{m.Subject}'");
            //    m = subSync.NextMessage(100);
            //}

            //catch (NATSTimeoutException)
            //{
            //    Console.WriteLine($"Sync subscription no messages currently available");
            //}


            //c.Drain();
        }

        public static bool ByteArrayToFile(string fileName, byte[] byteArray)
        {
            try
            {
                using (var fs = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                {
                    fs.Write(byteArray, 0, byteArray.Length);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception caught in process: {0}", ex);
                return false;
            }
        }
    }
}
