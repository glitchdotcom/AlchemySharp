using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlServerCe;
using System.IO;
using System.Linq;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using AlchemySharp.Embed.Dapper;

namespace AlchemySharp.Test {
    [TestClass]
    public class Examples {
        string GetTemporaryDirectory() {
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }

        private static readonly string FIXTURES = @"
            create table People (id int identity, name nvarchar(255));
            insert into People (name) values ('Donatello');
            insert into People (name) values ('Leonardo');
            insert into People (name) values ('Michelangelo');
            insert into People (name) values ('Raphael');

            create table Posts (id int identity, title nvarchar(255), author int);
            insert into Posts (title, author) values ('How to Make Money Fast!', 1);
            insert into Posts (title, author) values ('How to Make Money Slowly!', 2);
            insert into Posts (title, author) values ('How to Make Money the Easy Way!', 1);
            insert into Posts (title, author) values ('How to Make Money the Hard Way!', 2);
        ";

        private string Temp() {
            var path = Path.GetTempFileName();
            File.Delete(path);
            return path.Replace(".tmp", ".sdf");
        }

        private string CreateDB(string path) {
            using (var engine = new SqlCeEngine(string.Format(@"DataSource=""{0}""", path))) {
                engine.CreateDatabase();
                return engine.LocalConnectionString;
            }   
        }

        [TestInitialize]
        public void SetUp() {
            dbPath = Temp();
            connection = new SqlCeConnection(CreateDB(dbPath));
            db = new DB(connection);

            Posts = db["Posts"];
            People = db["People"];

            connection.ExecuteBatch(FIXTURES);
        }

        public void TearDown() {
            db.Dispose();
            File.Delete(dbPath);
        }

        private string dbPath;
        private DbConnection connection;
        private DB db;
        private Table People;
        private Table Posts;

        [TestMethod]
        public void TestSelect() {
            var results = db.Query(db["People"].All())
                .From(db["People"])
                .OrderBy(db["People"]["name"].Desc())
                .Execute()
                .Select(person => person.name);

            Assert.AreEqual(4, results.Count());
            Assert.AreEqual("Raphael", results.First());
        }

        [TestMethod]
        public void TestJoin() { 
            var posts = db["Posts"];
            var people = db["People"];

            var results = db.Query(people["name"], posts.All())
                .From(posts)
                .Join(people.On(people["id"] == posts["author"]))
                .OrderBy(people["name"].Desc(), posts["id"])
                .Execute()
                .Select(row => new { Author = row.name, Title = row.title });

            Assert.AreEqual(4, results.Count());
            Assert.AreEqual("How to Make Money Slowly!", results.First().Title);
        }

        [TestMethod]
        public void TestParameters() {
            var posts = db["Posts"];
            var people = db["People"];

            var results = db.Query(posts.All())
                .From(posts)
                .Where(posts["title"] == "How to Make Money Slowly!")
                .Execute()
                .Select(post => post.title);

            Assert.AreEqual(1, results.Count());
            Assert.AreEqual("How to Make Money Slowly!", results.First());
        }

        [TestMethod]
        public void TestApply() { 
            Func<Query, Query> permissions = (query) => 
                query.Where(Posts["id"] == 1);

            var results = db.Query(Posts.All())
                .From(Posts)
                .Apply(permissions)
                .Execute();

            Assert.AreEqual(1, results.Count());
        }

        [TestMethod]
        public void TestInjection() {
            var queries = new [] {
                new {
                    SQL = db.Query(People[@"id"", name"]).From(People),
                    Quoted = @"""People"".""id"""", name"
                },
                new {
                    SQL = db.Query(People["id"].As(@"id"", name")).From(People),
                    Quoted = @"""People"".""id"" as ""id"""", name"
                },
                new {
                    SQL = db.Query(People["id"]).From(People).Where(People["name"] == @""" OR 1 == 1"),
                    Quoted = @"(""People"".""name"" = @p0)"
                }
            };


            foreach (var query in queries) {
                var sql = query.SQL.ToSQL();
                Assert.IsTrue(sql.Contains(query.Quoted));
            }
        }

        private T Throws<T>(Action action) where T : Exception {
            try {
                action();
            } catch (T ex) { return ex;  }

            Assert.Fail("Failed to throw {0}.", typeof(T));
            return null;
        }
    }

    static class Extensions {
        public static void ExecuteBatch(this IDbConnection connection, string sql) {
            var commands = sql.Split(';')
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList();

            foreach (var command in commands) {
                connection.Execute(command);
            }
        }
    }
}
