using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces.Directory
{
    public interface ICommonDirectoryGrain
    {
        Task<int> Ping();

        Task Reset();
    }
}
