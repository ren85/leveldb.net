using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LevelDB.Tests
{
    [TestFixture]
    public class ReactiveTests
    {
        static string testPath = @"C:\Temp\Test";
        static string CleanTestDB()
        {
            DB.Destroy(testPath, new Options { CreateIfMissing = true });
            return testPath;
        }

        [Test]
        [ExpectedException(typeof(UnauthorizedAccessException))]
        public void TestOpen()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
            }

            using (var db = new DB(path, new Options { ErrorIfExists = true }))
            {
            }
        }

        [Test]
        public void TestCRUD()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                db.Put("Tampa", "green");
                db.Put("London", "red");
                db.Put("New York", "blue");

                Assert.AreEqual(db.Get("Tampa"), "green");
                Assert.AreEqual(db.Get("London"), "red");
                Assert.AreEqual(db.Get("New York"), "blue");

                db.Delete("New York");

                Assert.IsNull(db.Get("New York"));

                db.Delete("New York");
            }
        }

        [Test]
        public void TestRepair()
        {
            TestCRUD();
            DB.Repair(testPath, new Options());
        }

        [Test]
        public void TestIterator()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                db.Put("Tampa", "green");
                db.Put("London", "red");
                db.Put("New York", "blue");

                var expected = new[] { "London", "New York", "Tampa" };

                var actual = new List<string>();
                using (var iterator = db.CreateIterator(new ReadOptions()))
                {
                    iterator.SeekToFirst();
                    while (iterator.IsValid())
                    {
                        var key = iterator.GetStringKey();
                        actual.Add(key);
                        iterator.Next();
                    }
                }

                CollectionAssert.AreEqual(expected, actual);

            }
        }

        [Test]
        public void TestEnumerable()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                db.Put("Tampa", "green");
                db.Put("London", "red");
                db.Put("New York", "blue");

                var expected = new[] { "London", "New York", "Tampa" };
                var actual = from kv in db as IEnumerable<KeyValuePair<string, string>>
                             select kv.Key;

                CollectionAssert.AreEqual(expected, actual.ToArray());
            }
        }

        [Test]
        public void TestSnapshot()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                db.Put("Tampa", "green");
                db.Put("London", "red");
                db.Delete("New York");

                using (var snapShot = db.CreateSnapshot())
                {
                    var readOptions = new ReadOptions { Snapshot = snapShot };

                    db.Put("New York", "blue");

                    Assert.AreEqual(db.Get("Tampa", readOptions), "green");
                    Assert.AreEqual(db.Get("London", readOptions), "red");

                    // Snapshot taken before key was updates
                    Assert.IsNull(db.Get("New York", readOptions));
                }

                // can see the change now
                Assert.AreEqual(db.Get("New York"), "blue");

            }
        }

        [Test]
        public void TestGetProperty()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                var r = new Random(0);
                var data = "";
                for (var i = 0; i < 1024; i++)
                {
                    data += 'a' + r.Next(26);
                }

                for (int i = 0; i < 5 * 1024; i++)
                {
                    db.Put(string.Format("row{0}", i), data);
                }

                var stats = db.PropertyValue("leveldb.stats");

                Assert.IsNotNull(stats);
                Assert.IsTrue(stats.Contains("Compactions"));
            }
        }

        [Test]
        public void TestWriteBatch()
        {
            var path = CleanTestDB();

            using (var db = new DB(path, new Options { CreateIfMissing = true }))
            {
                db.Put("NA", "Na");

                using (var batch = new WriteBatch())
                {
                    batch.Delete("NA")
                         .Put("Tampa", "Green")
                         .Put("London", "red")
                         .Put("New York", "blue");
                    db.Write(batch);
                }

                var expected = new[] { "London", "New York", "Tampa" };
                var actual = from kv in db as IEnumerable<KeyValuePair<string, string>>
                             select kv.Key;

                CollectionAssert.AreEqual(expected, actual.ToArray());
            }
        }
    }
}
