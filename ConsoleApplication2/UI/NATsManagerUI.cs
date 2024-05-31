////////////////////////////////////////////////////////////////////////////////////////////////////////
//FileName: NATsManagerUI.cs
//FileType: Visual C# Source file
//Author : Nouman Nawaz
//Created On : 18/05/2024 9:56:39 AM
//Copy Rights : Avanza Solutions
//Description : Class for managing UI configs, initialization, start, stop.
////////////////////////////////////////////////////////////////////////////////////////////////////////
using System;
using NATS.Client;
using NATS.Client.JetStream;
using System.Threading;
using Aspire.Core.Logging;
using System.Text;
using System.Configuration;


namespace Aspire.Core
{
    public partial class NatsManager
    {
        public const string c_WRITERSRC_USERINTERFACE = "userinterface";
        public const int c_NATS_DEFAULT_TIMEOUT = 5000;
        private const int c_REQUEST_REPLY_TIMEOUT = 5000;
        private static int noOfUIThreads;

        private System.Threading.Thread natsUIPushSubscriberSyncThread;

        public NATsQueueUI natsQueueUI;
        public static IAsyncSubscription NatsAsyncSubscription = null;
        private static IJetStreamPushSyncSubscription[] UISubscriberList;


        public void InitUI(Aspire context)
        {
            natsQueueUI = new NATsQueueUI(context, ref natsServerConnected);
        }

        public void StartUI()
        {
            noOfUIThreads = int.Parse(ConfigurationManager.AppSettings["noOfUISubscribers"]);
            UISubscriberList = new IJetStreamPushSyncSubscription[noOfUIThreads];
            natsQueueUI.Initialize(noOfUIThreads, ref UISubscriberList);

            for (int i = 0; i < noOfUIThreads; i++)
            {
                natsUIPushSubscriberSyncThread = new System.Threading.Thread(new ParameterizedThreadStart(natsQueueUI.UIPushSubscriberSync));
                natsUIPushSubscriberSyncThread.IsBackground = true;
                natsUIPushSubscriberSyncThread.Start(i);
            }

            NatsAsyncSubscription = NatsManager.getNatsConnection().SubscribeAsync(String.Concat(NATsParams.Stream.ASPIREWEBUI, ".*"), natsQueueUI.SubscribeAsyncInboxHandler);
        }

        public void StopUI()
        {
            for (int i = 0; i < noOfUIThreads; i++)
            {
                if ((UISubscriberList[i] != null) && (UISubscriberList[i].IsValid))
                    UISubscriberList[i].Unsubscribe();
            }

            if (NatsAsyncSubscription != null)
                NatsAsyncSubscription.Unsubscribe();
        }

        public static void PublishUI(string stream, string queue_subject, UIQueueMessage message)
        {
            int threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in PublishMessage Function");

            try
            {
                if (stream == string.Empty)
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Queue name is not specified, Nats publish failed.");


                if (NatsManager.IsNatsConnected())
                {
                    try
                    {
                        queue_subject = String.Concat(stream, ".", queue_subject);

                        var itm = Newtonsoft.Json.JsonConvert.SerializeObject(message);

                        byte[] payload = Encoding.ASCII.GetBytes(itm);

                        MsgHeader msg = new MsgHeader();
                        msg.Add("WriterSource", c_WRITERSRC_USERINTERFACE);
                        Msg natsMessage = new Msg(queue_subject, msg, payload);
                        NatsManager.getNatsConnection().Publish(natsMessage);
                        Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, string.Format("Published Message over {0} : Nats Subject: {1}", stream, queue_subject));
                    }
                    catch (Exception ex)
                    {
                        Logger.Instance.Log(LogSectionType.AspireUI, LogType.Error, string.Format("Error: Failed to publish message. {0}", ex.Message));
                    }
                }
                else
                {
                    // what to do when nats is not connected ?? wait or retry
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Nats server is not connected");
                    System.Threading.Thread.Sleep(1000);
                }
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Error, ex.Message);
            }
        }
    }
}