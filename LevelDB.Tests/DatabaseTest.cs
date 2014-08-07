using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LevelDB.Tests
{
    [TestFixture]
    public class DatabaseTest
    {
        [Test]
        public void Intro()
        {
            using (var database = new DB("mytestdb", new Options() { CreateIfMissing = true }))
            {
                database.Put("key1", "value1");
                Assert.AreEqual("value1", database.Get("key1"));
                Assert.IsTrue(database.Get("key1") != null);
                database.Delete("key1");
                Assert.IsFalse(database.Get("key1") != null);
                Assert.IsNull(database.Get("key1"));
            }
        }
    }
}
