using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LevelDB
{
    public class BloomFilterPolicy : LevelDBHandle
    {
        public BloomFilterPolicy(int bits_per_key)
        {
            Handle = LevelDBInterop.leveldb_filterpolicy_create_bloom(bits_per_key);
        }

        protected override void FreeUnManagedObjects()
        {
            LevelDBInterop.leveldb_filterpolicy_destroy(Handle);
        }
    }
}
