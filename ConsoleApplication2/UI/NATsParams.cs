////////////////////////////////////////////////////////////////////////////////////////////////////////
//FileName: NATsParam.cs
//FileType: Visual C# Source file
//Author : Nouman Nawaz
//Created On : 18/05/2024 9:56:39 AM
//Copy Rights : Avanza Solutions
//Description : Class for handling params.
////////////////////////////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aspire.Core
{
    public static class NATsParams
    {
        public enum Stream { ASPIREWEBUI };
        //public enum Subject { FREE_REQUESTS, AWAIT_REQUESTS, AWAIT_RESPONSE };
        public enum Subject { FREE_REQUESTS, REQUEST_REPLY };
    }
}
