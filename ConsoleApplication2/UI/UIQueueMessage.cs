using System;
using System.Collections.Generic;

namespace Aspire.Core
{
    [Serializable]
    public class MethodParam
    {
        public Type ParamType { get; set; }
        public string ParamName { get; set; }
        public object ParamValue { get; set; }
    }

    [Serializable]
    public class UIQueueMessage
    {
        #region Properties

        public string MethodName { get; set; }
        public List<MethodParam> ParamList { get; set; }
        public MethodParam ParamReturn { get; set; }

        #endregion

        public UIQueueMessage(string methodName)
        {
            this.MethodName = methodName;
        }


        public void AddParam(Type paramType, string paramName, object paramValue)
        {
            if (this.ParamList == null)
                this.ParamList = new List<MethodParam>();

            this.ParamList.Add(new MethodParam()
            {
                ParamType = paramType,
                ParamName = paramName,
                ParamValue = paramValue
            });
        }


        public void AddReturnParam(Type paramType, object paramValue)
        {
            this.ParamReturn = new MethodParam()
            {
                ParamType = paramType,
                ParamValue = paramValue
            };
        }
    }

}
