////////////////////////////////////////////////////////////////////////////////////////////////////////
//FileName: NATsUIUtils.cs
//FileType: Visual C# Source file
//Author : Nouman Nawaz
//Created On : 18/05/2024 9:56:39 AM
//Copy Rights : Avanza Solutions
//Description : Class for handling UI Utils consists of WebService functions.
////////////////////////////////////////////////////////////////////////////////////////////////////////
using Aspire.Core.Logging;
using Aspire.Core.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aspire.Core
{
    public class NatsUIUtils
    {

        public bool ProcessBatch(string pBatchID, string ValueDate, string BIC)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessBatch");
            bool res = false;
            //int batchNum = int.Parse(pBatchID);
            res = BatchHelper.ProcessBatch(pBatchID, ValueDate, BIC);

            return res;
        }
        public bool ProcessBatchRefund(string pBatchID, string ValueDate, string BIC)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessBatchRefund");
            bool res = false;
            //int batchNum = int.Parse(pBatchID);
            res = BatchHelper.ProcessBatchRefund(pBatchID, ValueDate, BIC);

            return res;
        }

        public bool ProcessPDBatch(string pBatchID, string ValueDate, string BIC)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessPDBatch");
            bool res = false;
            //int batchNum = int.Parse(pBatchID);
            res = BatchPDHelper.ProcessPDBatch(pBatchID, ValueDate, BIC);

            return res;
        }

        public bool ProcessBillerList(string JsonResponse)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessBillerList");
            bool res = false;
            //int batchNum = int.Parse(pBatchID);
            res = BillerListHelper.ProcessBillerList(JsonResponse);



            return res;
        }

        public void ProcessCancelation(string pTransactionID)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessCancelation [Not Implemented]");
        }

        public void ProcessDispute(string pTransactionID)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDispute [Not Implemented]");
        }

        public void ProcessReturns(DataSet RetDS, bool IsNRT)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessReturns");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessReturns");
                BatchHelper.ProcessReturns(RetDS, IsNRT);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Exception occured WebServiceAspire.ProcessReturns " + ex.Message);
            }
        }

        public void ProcessDebitTrans(DataSet RetMDS)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDebitTrans");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessDebitTrans");
                BatchHelper.ProcessDebitTrans(RetMDS);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessDebitTrans " + ex.Message);
            }
        }

        public void ProcessDefCredit_PSP(DataSet DefDSC)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDefCredit_PSP");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "PSP: Entered function WebServiceAspire.ProcessDefCredit_PSP");
                BatchHelper.ProcessDefCredit_PSP(DefDSC);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessDefCredit_PSP " + ex.Message);
            }
        }

        public void ProcessDefCredit(DataSet DefDSC)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDefCredit");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessDefCredit");
                BatchHelper.ProcessDefCredit(DefDSC);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessDefCredit " + ex.Message);
            }
        }

        public void ProcessDefBillPayment(DataSet DefDSBP)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDefBillPayment");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessDefBillPayment");
                BatchHelper.ProcessDefBillPayment(DefDSBP);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessDefBillPayment " + ex.Message);
            }
        }
        public void ProcessDefBillPayment_CORP(DataSet DefDSBP)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDefBillPayment_CORP");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function NatsUIUtils.ProcessDefBillPayment_CORP");
                BatchHelper.ProcessDefBillPayment_CORP(DefDSBP);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured NatsUIUtils.ProcessDefBillPayment_CORP " + ex.Message);
            }
        }

        public void ProcessDefDebit(DataSet DefDSD)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessDefDebit");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessDefDebit");
                BatchHelper.ProcessDefDebit(DefDSD);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessDefDebit " + ex.Message);
            }

        }

        public void ProcessCancelledTranSB(DataSet CANds)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessCancelledTranSB");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessCancelledTranSB");
                BatchHelper.ProcessSingleCancellation(CANds);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessCancelledTranSB " + ex.Message);
            }

        }

        public void ProcessCancelledTranMB(DataSet CANds)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function ProcessCancelledTranMB");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.ProcessCancelledTranMB");
                BatchHelper.ProcessMultipleCancellation(CANds);
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.ProcessCancelledTranMB " + ex.Message);
            }

        }

        public void SendTextMsg(string pTextMessageCode, string pTextMessage)
        {
            try
            {
                Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function SendTextMsg");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function SendTextMsg");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Incoming MessageText: " + pTextMessage + " and Code: " + pTextMessageCode);
                string[] msgArr = new string[1];
                msgArr[0] = pTextMessage;
                QueueMessage qMsg = new QueueMessage() { MessageKey = pTextMessageCode, Message = pTextMessage };
                string outRef = string.Empty;
                QueueMessage objn99 = null;
                objn99 = MessageConverter.GetSwiftMessage(msgArr, qMsg, "MTn99", out outRef);
                //Send to Benefit
                if (objn99 != null)
                {
                    //ThreadPool.Instance.QueueWorkItem(ThreadPoolTypeEnum.NRT_SWIFT, objn99);
                    NatsManager.Publish(NatsQueues.Q_All.Name, NatsQueues.Q_All.Subjects.S_NRT_SWIFT, objn99); // Hiba
                    Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "999 Message Published over NATS");
                }
                else
                    Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Empty Queue Msg returned");
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Exited function SendTextMsg");
            }
            catch (Exception ex)
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Error occured in function SendTextMsg: " + ex.Message);
            }
        }

        //public DataSet GetSystemMonitoringData()
        //{
        //    try
        //    {
        //        Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function GetSystemMonitoringData");
        //        string[] MonitoringFields = new string[] { "BENEFIT_CONNECTIVITY", "ICM_CONNECTIVITY", "ORACLE_CONNECTIVITY", "PHOENIX_CONNECTIVITY" };
        //        DataSet dsMon = new DataSet();
        //        DataTable SysMon = new DataTable();
        //        SysMon.Columns.Add("MODULE");
        //        SysMon.Columns.Add("STATUS");
        //        string benefitStatus = TCPHelper.GetBenefitStatus() == "0" ? "Connected" : "Disconnected";
        //        string icmStatus = WebServiceAspire.AspireContext["RDVTCP"].GetICMConnectivity() == true ? "Connected" : "Disconnected";
        //        string OracleCon = TCPHelper.CheckOracleConnection() == true ? "Connected" : "Disconnected";
        //        string SybaseCon = TCPHelper.CheckSybaseConnection() == true ? "Connected" : "Disconnected";
        //        SysMon.Rows.Add(MonitoringFields[0], benefitStatus);
        //        SysMon.Rows.Add(MonitoringFields[1], icmStatus);
        //        SysMon.Rows.Add(MonitoringFields[2], OracleCon);
        //        SysMon.Rows.Add(MonitoringFields[3], SybaseCon);
        //        dsMon.Tables.Add(SysMon);
        //        Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Exited function GetSystemMonitoringData");
        //        return dsMon;
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Error occured in function GetSystemMonitoringData: " + ex.Message);
        //        return null;
        //    }
        //}

        public void RefreshParticipantAccount()
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function RefreshParticipantAccount");
            CacheItem.Instance.CacheEFTSParticipantAccounts();
        }

        //Muhammad Bilal 21/02/21 @12:46pm
        //Sync Bilal Cache
        public void SynBillerCache(string positionAccount, string ledgerAccount, string tranType)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function SynBillerCache");
            CacheItem.Instance.AddNewRow(positionAccount, ledgerAccount, tranType);
        }
        //Muhammad Bilal 22/02/21 @08:48am
        //Sync Bilal Cache
        public void DeleteBillerCache(string positionAccount)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function DeleteBillerCache");
            CacheItem.Instance.DeleteRow(positionAccount);
        }


        #region DISPUTE HANDLER


        public int GetSwitchStatus()
        {
            int retVal = 0;

            try
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.GetSwitchStatus");
                retVal = UIHelper.GetSwitchStatus();
            }
            catch (Exception ex)
            {
                retVal = (int)ServiceStatus.LogOff;
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.GetSwitchStatus " + ex.Message);
            }

            return retVal;
        }

        public int UpdateSwitchStatus(object _status)
        {
            int retVal = (int)ReturnCode.Failed;
            int status = Convert.ToInt32(_status);

            try
            {
                Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Entered function WebServiceAspire.UpdateSwitchStatus");
                retVal = UIHelper.UpdateSwitchStatus(status);
            }
            catch (Exception ex)
            {
                retVal = (int)ReturnCode.Failed;
                Logger.Instance.Log(LogSectionType.Swift, LogType.Error, "Exception occured WebServiceAspire.UpdateSwitchStatus " + ex.Message);
            }
            return retVal;
        }

        public string GetBussinessDaySession()
        {
            //return DateTime.Now.ToString("yyyyMMddhhmmss");
            if (BusinessDayManager.Instance.CURRENT_BUSINESS_PERIOD == null || string.IsNullOrEmpty(BusinessDayManager.Instance.CURRENT_BUSINESS_PERIOD.Period))
            {
                return "Request Business Day.";
            }

            return string.Format("{0}:{1}", BusinessDayManager.Instance.CURRENT_BUSINESS_PERIOD.BusinessDay, BusinessDayManager.Instance.CURRENT_BUSINESS_PERIOD.Period);
        }

        #endregion

        public bool FetchBillerList()
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function FetchBillerList");
            Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "Fetching Biller List from Aspire UI");
            string json = "";
            BillerListHelper.ProcessBillerList(json);
            return true;
        }

        public bool CorporatePaymentRefundFromUI_1(string _pipeMsg)
        {
            Logger.Instance.Log(LogSectionType.AspireUI, LogType.Info, "Entered function CorporatePaymentRefundFromUI_1");
            Logger.Instance.Log(LogSectionType.Swift, LogType.Info, "CorporatePaymentRefundFromUI_1 from Aspire UI with Pipe:[" + _pipeMsg + "]");
            TCPService.HandleRefundRequestFrom_UI(_pipeMsg);
            return true;
        }
    }
}
