////////////////////////////////////////////////////////////////////////////////////////////////////////
//FileName: NATsQueueUI.cs
//FileType: Visual C# Source file
//Author : Nouman Nawaz
//Created On : 18/05/2024 9:56:39 AM
//Copy Rights : Avanza Solutions
//Description : Class for handling UI events over NATS, this class incorporates publish/sub and config.
////////////////////////////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using Aspire.Core.Logging;
using System.Reflection;
using System.Configuration;
using Microsoft.Win32;

namespace Aspire.Core
{
    public class NATsQueueUI : AbstractService
    {
        public static string sNatsStream;
        public static string sNatsSubject;
        private static StorageType StorageType;
        private static DiscardPolicy DiscardPolicy;
        private static RetentionPolicy RetentionPolicy;
        private static bool DenyDelete;
        private static bool AllowRollup;
        private static int Replicas;
        private static int MessageLimit;
        private static int MessageLimitPerSubject;
        private static int TotalStreamSize;
        private static int MessageTTL;
        private static int MaxMessageSize;
        private static int DuplicateWindow;
        private static bool AllowPurge;

        private static IJetStream natsJetStream;
        private static IJetStreamManagement natsJsManager;
        private static StreamConfiguration natsStreamConfig;
        private static ConsumerConfiguration natsConsumerConfig;
        private static PushSubscribeOptions natsPushSubscribeOpts;
        private static IJetStreamPushSyncSubscription[] natsUISubsribersList;

        private static int noOfConsumers;

        private static Aspire aspireContext;
        private static StreamInfo natsStreamInfo;
        private static object hold = new object();

        public NATsQueueUI(Aspire context, ref bool natsConnStatus) :
            base(context)
        {
            aspireContext = context;
            //Queue Name is Directly fetched from config in case of UI.
            natsConnStatus = NatsManager.IsNatsConnected();
        }

        public NATsQueueUI(AspireBckGroundServiceManager context) :
            base(context)
        {

        }

        private void Instance_OnLog(string pMessage)
        {
            Logger.Instance.Log(LogSectionType.DCert, LogType.Info, pMessage);
        }

        public void Initialize(int subscriberCount, ref IJetStreamPushSyncSubscription[] pNatsSubscribersList)
        {

            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in Initialize Function.");
            try
            {
                // configuring nats connection variable from config file and registry
                // stream and subject name is same as queue name received from registry
                sNatsStream = System.Configuration.ConfigurationManager.AppSettings["sNatsUIStream"];
                sNatsSubject = System.Configuration.ConfigurationManager.AppSettings["sNatsUISubjects"];
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Nats sNatsStream is not found ");
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Error: " + ex.Message);
            }

            int threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            natsUISubsribersList = pNatsSubscribersList;
            ConfigureNatsUI(threadId);
        }

        private void ConfigureNatsUI(int threadId)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in InitializeNatsUI function, Thread [" + threadId + "].");
            try
            {
                // defining stream configurations
                String TempStorageType = System.Configuration.ConfigurationManager.AppSettings["StorageType"];
                StorageType = (StorageType)Enum.Parse(typeof(StorageType), TempStorageType);
                String TempDiscard = System.Configuration.ConfigurationManager.AppSettings["DiscardPolicy"];
                DiscardPolicy = (DiscardPolicy)Enum.Parse(typeof(DiscardPolicy), TempDiscard);
                String TempRetention = System.Configuration.ConfigurationManager.AppSettings["UIRetentionPolicy"];
                RetentionPolicy = (RetentionPolicy)Enum.Parse(typeof(RetentionPolicy), TempRetention);
                DenyDelete = bool.Parse(System.Configuration.ConfigurationManager.AppSettings["DenyDeletion"]);
                AllowRollup = bool.Parse(System.Configuration.ConfigurationManager.AppSettings["AllowRollup"]);
                Replicas = int.Parse(System.Configuration.ConfigurationManager.AppSettings["Replicas"]);
                MessageLimit = int.Parse(System.Configuration.ConfigurationManager.AppSettings["MessageLimit"]);
                MessageLimitPerSubject = int.Parse(System.Configuration.ConfigurationManager.AppSettings["MessageLimitPerSubject"]);
                TotalStreamSize = int.Parse(System.Configuration.ConfigurationManager.AppSettings["TotalStreamSize"]);
                MessageTTL = int.Parse(System.Configuration.ConfigurationManager.AppSettings["MessageTTL"]);
                MaxMessageSize = int.Parse(System.Configuration.ConfigurationManager.AppSettings["MaxMessageSize"]);
                DuplicateWindow = int.Parse(System.Configuration.ConfigurationManager.AppSettings["DuplicateWindow"]);
                AllowPurge = bool.Parse(System.Configuration.ConfigurationManager.AppSettings["AllowPurge"]);
                int MaxPendingAcks = int.Parse(System.Configuration.ConfigurationManager.AppSettings["MaxAckPending"]);
                int MaxAckWait = int.Parse(System.Configuration.ConfigurationManager.AppSettings["AckWait"]);



                natsStreamConfig = StreamConfiguration.Builder()
                        .WithName(sNatsStream)
                        //.WithSubjects(sNatsSubject)
                        .WithSubjects(sNatsSubject.Split(',').ToList<string>())
                        .WithStorageType(StorageType)
                        .WithDenyDelete(DenyDelete)
                        .WithAllowRollup(AllowRollup)
                        .WithDiscardPolicy(DiscardPolicy)
                        .WithRetentionPolicy(RetentionPolicy)
                        .WithMaxAge(MessageTTL)
                        .WithReplicas(Replicas)
                        .WithMaxMsgSize(MaxMessageSize)
                        .WithMaxMessages(MessageLimit)
                        .WithMaxMessagesPerSubject(MessageLimitPerSubject)
                        .WithDuplicateWindow(DuplicateWindow)
                        .WithMaxBytes(TotalStreamSize)
                        .Build();

                // defining consumer configuration
                    natsConsumerConfig = ConsumerConfiguration.Builder()
                                            /*.WithName(sNatsStream + NatsManager.sNodeName)*/
                                            .WithAckPolicy(AckPolicy.Explicit)
                                            .WithDurable(sNatsStream + "Durable_" + NatsManager.sNodeName)
                                            .WithDeliverGroup(sNatsStream + "Group")
                                            .WithMaxAckPending(MaxPendingAcks)
                                            .WithAckWait(MaxAckWait)
                                            .Build();
               
                    natsPushSubscribeOpts = PushSubscribeOptions.Builder()
                   .WithStream(sNatsStream)
                   .WithConfiguration(natsConsumerConfig)
                   .Build();


              

                IConnection nats = NatsManager.getNatsConnection();
                natsJsManager = nats.CreateJetStreamManagementContext();
                try
                {
                    natsStreamInfo = natsJsManager.GetStreamInfo(sNatsStream); // this throws if the stream does not exist
                }
                catch (NATSJetStreamException)
                {
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Nats stream [" + sNatsStream + "] does not exist.");
                }

                if (natsStreamInfo == null)
                {
                    natsStreamInfo = natsJsManager.AddStream(natsStreamConfig);
                }
                natsJetStream = NatsManager.getNatsConnection().CreateJetStreamContext();
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Error: " + ex.Message);
                throw ex;
            }
        }

        public void RefreshSubscriptionUI(int threadId, ref IJetStreamPushSyncSubscription subscriberHandle)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in RefreshSubscriptionUI Function, Thread [" + threadId + "].");
            try
            {
                lock (hold)
                {
                    if (!NatsManager.IsNatsConnected())
                    {
                        Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "--- Unable to connect to Nats server ---");
                    }

                    if (NatsManager.IsNatsConnected())
                    {
                        //subscriberHandle = natsJetStream.PushSubscribeSync(sNatsSubject, sNatsStream + "Group", natsPushSubscribeOpts);
                        subscriberHandle = natsJetStream.PushSubscribeSync(sNatsStream + ".>", sNatsStream + "Group", natsPushSubscribeOpts);
                        if (subscriberHandle != null)
                        {
                            //natsServerConnected = true;
                            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Nats stream [" + sNatsStream + "] subscription successful on NATS server.");
                        }
                        else
                        {
                            //natsServerConnected = false;
                            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Nats stream [" + sNatsStream + "] subscription unsuccessful on NATS server.");
                        }
                    }
                }
                if (!NatsManager.IsNatsConnected())
                {
                    System.Threading.Thread.Sleep(1000);
                }
            }
            catch (Exception exception)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Error in Subscriber: " + exception.Message);
            }
        }

        public void UIPushSubscriberSync(object param)
        {
            int threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            int index = int.Parse(param.ToString());


            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, string.Format("Entered in UIPushSubscriberSync Function, Thread [{0}].", threadId));

            IJetStreamPushSyncSubscription natsJsSyncSubHandle = natsUISubsribersList[index];

            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, string.Format("natsServerConnected state : {0}", NatsManager.IsNatsConnected()));

            try
            {
                while (!NatsManager.IsNatsConnected())
                {
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Waiting for nats connection...");
                    System.Threading.Thread.Sleep(1000);
                }

                RefreshSubscriptionUI(threadId, ref natsJsSyncSubHandle);

                while (true)
                {
                    Msg msg = null;
                    bool foundJsMsg = FetchNewMessage(threadId, ref natsJsSyncSubHandle, out msg);

                    if (foundJsMsg)
                    {
                        foundJsMsg = ProcessMessage(ref msg);
                    }
                }
            }
            catch (System.Threading.ThreadAbortException ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Error, "Aborted the thread.");
                return;
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Error, "Stopped the thread.");
            }
        }

        private bool ProcessMessage(ref Msg msg)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in Method ProcessMessage");
            bool foundJsMsg = true;

            try
            {
                string payload = Encoding.UTF8.GetString(msg.Data);

                if (msg.Header["WriterSource"].ToUpper().Equals(NatsManager.c_WRITERSRC_USERINTERFACE.ToUpper()))
                {
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, string.Format("Message received from UI - Incoming Subject: {0}", msg.Subject));
                    UIQueueMessage qMessage = Newtonsoft.Json.JsonConvert.DeserializeObject<UIQueueMessage>(payload);
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Calling Method: " + qMessage.MethodName);
                    ExecuteMethod(typeof(NatsUIUtils), qMessage.MethodName, qMessage.ParamList);
                    msg.Ack(); //Ack should be done after message is processed.
                }
                else
                {
                    Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Message received from Unknown source. Unable to process incoming message");
                }

                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Message processed and acknowledged ");
                foundJsMsg = false;
                msg = null;
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Error, string.Format("Error: Failed to parse message: {0}", ex.Message));
                System.Threading.Thread.Sleep(1000);
            }

            return foundJsMsg;
        }

        private bool FetchNewMessage(int threadId, ref IJetStreamPushSyncSubscription natsJsSyncSubHandle, out Msg msg)
        {
            bool foundJsMsg = false;
            //sLogger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered in Method FetchNewMessage");

            msg = null;

            if (NatsManager.IsNatsConnected())
            {
                if (!foundJsMsg)
                {
                    try
                    {
                        if (natsJsSyncSubHandle != null)
                        {
                            //if (natsJsSyncSubHandle.DeliverSubject.ToUpper().StartsWith("_INBOX."))
                            //{
                            //    foundJsMsg = false;
                            //    return foundJsMsg;
                            //}

                            msg = natsJsSyncSubHandle.NextMessage(NatsManager.c_NATS_DEFAULT_TIMEOUT); // next msg timeout 
                            foundJsMsg = true;

                            //msg.Ack();
                        }
                    }
                    catch (NATSTimeoutException ex)
                    {
                        System.Threading.Thread.Sleep(200);
                    }
                    catch (Exception ex)
                    {
                        foundJsMsg = false;
                        System.Threading.Thread.Sleep(1000);
                    }
                }
                else
                {
                    // Some message found at NATS Server
                    System.Threading.Thread.Sleep(100);
                }
            }
            else
            {
                // NATS Server disconnected
                RefreshSubscriptionUI(threadId, ref natsJsSyncSubHandle);
            }

            return foundJsMsg;
        }

        public void SubscribeAsyncInboxHandler(object sender, MsgHandlerEventArgs args)
        {
            int threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, string.Format("Entered in SubscribeAsyncInboxHandler Function, Thread [{0}].", threadId));

            if (String.IsNullOrEmpty(args.Message.Reply))
            {
                return;
            }

            if (args.Message.HasHeaders && args.Message.Header["WriterSource"].ToUpper().Equals(NatsManager.c_WRITERSRC_USERINTERFACE.ToUpper()))
            {
                string response = String.Empty;

                var payload = Encoding.ASCII.GetString(args.Message.Data);

                if (args.Message.Subject.ToUpper().Equals("ASPIREWEBUI.REQUEST_REPLY"))
                {
                    var qMessage = Newtonsoft.Json.JsonConvert.DeserializeObject<UIQueueMessage>(payload);
                    object retVal = ExecuteMethod(typeof(NatsUIUtils), qMessage.MethodName, qMessage.ParamList);

                    if (retVal != null)
                        qMessage.AddReturnParam(typeof(int), (object)retVal);

                    response = Newtonsoft.Json.JsonConvert.SerializeObject(qMessage);
                }
                else
                {
                    var qMessage = Newtonsoft.Json.JsonConvert.DeserializeObject<UIQueueMessage>(payload);
                    response = Newtonsoft.Json.JsonConvert.SerializeObject(qMessage);
                }

                NatsManager.getNatsConnection().Publish(args.Message.Reply, Encoding.UTF8.GetBytes(response));
                args.Message.Ack();
            }
        }

        private static object ExecuteMethod(Type className, string methodName, List<MethodParam> paramList)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            Type typeInstance = assembly.GetType(className.FullName);
            MethodInfo methodInfo = typeInstance.GetMethod(methodName);
            ParameterInfo[] parameterInfos = methodInfo.GetParameters();

            object result = null;

            int index = 0;
            object[] parameters = new object[parameterInfos.Length];

            if (paramList != null && paramList.Count > 0)
            {
                foreach (var pInfo in parameterInfos)
                {
                    var pValue = paramList.Find(x => x.ParamName.Equals(pInfo.Name))?.ParamValue;

                    if (pValue != null)
                    {
                        parameters[index] = pValue;
                    }

                    index++;
                }
            }

            if (typeInstance != null) //non static
            {
                if (methodInfo.IsStatic == false)
                {
                    //instance is needed to invoke the method
                    object classInstance = Activator.CreateInstance(typeInstance, null);

                    if (parameterInfos.Length == 0)
                    {
                        // there is no parameter we can call with 'null'
                        result = methodInfo.Invoke(classInstance, null);
                    }
                    else
                    {
                        result = methodInfo.Invoke(classInstance, parameters);
                    }
                }
                else //handle static
                {
                    if (parameterInfos.Length == 0)
                    {
                        // there is no parameter we can call with 'null'
                        result = methodInfo.Invoke(null, null);
                    }
                    else
                    {
                        result = methodInfo.Invoke(null, parameters);
                    }
                }
            }
            return result;
        }
    }

    public enum OperationType
    {
        ProcessBatch,
        ProcessBatchRefund,
        ProcessPDBatch,
        ProcessBillerList,
        ProcessCancelledTranSB,
        ProcessCancelledTranMB,
        ProcessDispute,
        ProcessDefCredit_PSP,
        ProcessDefCredit,
        ProcessDefBillPayment,
        ProcessDefBillPayment_CORP,
        ProcessDefDebit,
        ProcessReturns,
        ProcessDebitTrans,
        SendTextMsg,
        GetSwitchStatus,
        UpdateSwitchStatus,
        GetBussinessDaySession,
        RefreshParticipantAccount,
        SynBillerCache,
        DeleteBillerCache,
        FetchBillerList,
        CorporatePaymentRefundFromUI_1
    }

}