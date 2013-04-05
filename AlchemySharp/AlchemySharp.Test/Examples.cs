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

            create table Weather (id int identity, temperature int, description nvarchar(255));
            insert into Weather (temperature, description) values (20, 'sunny');
            insert into Weather (temperature, description) values (10, 'cloudy');
            insert into Weather (temperature, description) values (5, 'cloudy raining');
            insert into Weather (temperature, description) values (0, 'cloudy sleeting');
            insert into Weather (temperature, description) values (-5, 'cloudy snowing');
            insert into Weather (temperature, description) values (-10, 'clear cold');
            insert into Weather (temperature, description) values (-15, NULL);
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
        public void TestUnion() {
            var easy = db.Query(Posts["title"].As("description"))
                .From(Posts)
                .Where(Posts["title"].Contains("Easy"));

            var leonardo = db.Query(People["name"].As("description"))
                .From(People)
                .Where(People["name"].Contains("Leonardo"));

            var results = easy.Union(leonardo)
                .Execute()
                .Select(row => row.description);

            Assert.AreEqual(2, results.Count());

        }

        [TestMethod]
        public void TestSubqueries() {
            // Basically a join, done with a subquery
            var authors = db.Query(People["id"])
                .From(People)
                .Where(People["id"].In(1, 3));

            var results = db.Query(Posts.All())
                .From(Posts)
                .Where(Posts["author"].In(authors))
                .Execute();

            Assert.AreEqual(2, results.Count(), "Where in!");

            // ... and this one is done with a corelated subquery.
            results = db.Query(Posts.All())
                .From(Posts)
                .WhereExists(db.Query(People["id"])
                    .From(People)
                    .Where(People["id"] == Posts["author"])
                    .Where(People["id"].In(1, 3))
                )
                .Execute();

            Assert.AreEqual(2, results.Count(), "WhereExists!");
        }

        [TestMethod]
        public void TestGroupBy() {
            var results = db.Query(Posts["author"], Sql.Func.Max(Posts["title"]), Sql.Func.Count().As("count"))
                .From(Posts)
                .GroupBy(Posts["author"])
                .Execute();

            Assert.AreEqual(2, results.Count());
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

        [TestMethod]
        public void TestExpressions() { 
            var weather = db["Weather"];
            var results = db.Query(weather["temperature"])
                .From(weather)
                .Where(weather["temperature"] > 0)
                .Execute();

            Assert.AreEqual(3, results.Count());

            var cold = db.Query(weather.All())
                .From(weather)
                .Where(weather["temperature"] <= 0 & weather["description"].Contains("snow"))
                .Execute();

            Assert.AreEqual(1, cold.Count());

            var snowing = weather["description"].Contains("snow");
            var warm = weather["temperature"] >= 10;
            var nice = db.Query(weather.All())
                .From(weather)
                .Where(warm | snowing)
                .Execute();

            Assert.AreEqual(3, nice.Count());


            var nullness = db.Query(weather.All())
                .From(weather)
                .Where(weather["temperature"] < 0 & weather["description"].IsNotNull())
                .Execute();

            Assert.AreEqual(2, nullness.Count());

            nullness = db.Query(weather.All())
                .From(weather)
                .Where(weather["description"].IsNull())
                .Execute();

            Assert.AreEqual(1, nullness.Count());
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
