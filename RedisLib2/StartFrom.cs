using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLib2
{
    public enum StartFrom
    {
        Message,
        Offset,
        LastAcked,
        Head
    }
}
