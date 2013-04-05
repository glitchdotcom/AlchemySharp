AlchemySharp - SQL Query Builder Inspired by SQLAlchemy
=======================================================
AlchemySharp is a query building tool insired by [SQLAlchemy's Expression Language](http://docs.sqlalchemy.org/en/rel_0_8/core/tutorial.html). It allows you to write queries like:

    var results = db.Query(db["People"].All())
        .From(db["People"])
        .Execute()
        .Select(person => person.name);

The central idea of AlchemySharp is to model SQL giving you access to the full expressive power of SQL in the form of joins, unions, and subqueries without forcing you to mash giant strings together.

Getting Started
---------------
```AlchemySharp.DB``` is the central class, which you create by passing it a ```DbConnection```. Once you've got an instance, you can use that to build and run queries.

    using(var db = new DB(new SqlConnection(connectionString))) {
       var results = db.Query(db["People"].All())
            .From(db["People"])
            .Execute()
            .Select(person => person.name);
    }

Expressions
-----------
AlchemySharp overloads a number of C# operators to allow you to build up SQL expressions:

	var weather = db["Weather"];
	var cold = db.Query(weather.All())
		.From(weather)
		.Where(weather["temperature"] <= 0 & weather["description"].Contains("snow"))
		.Execute();

... here we see the ```<=``` and ```&``` operators in action. The full list of operators includes:

	==	Equal
	!=	Not equal
	<	Less than
	<=	Less than or equal
	>	Greater than
	>=	Greater than or equal
	+	Plus
	-	Minus
	&	And
	|	Or
	~	Not

Furthermore, AlchemySharp expressions implement a number of methods to help you build expressions:

	// substring matching
	.Like(string like)
	.StartsWith(string prefix)
	.EndsWith(string suffix)
	.Contains(string substring)

	// NULL handling
	.IsNull()
	.IsNotNull()

	// List handling
	.In(params object[] values)

**Helpful tip**: Expressions are just objects that can be held in variables which can help keep things readable:

	var snowing = weather["description"].Contains("snow");
	var warm = weather["temperature"] >= 10;
	var nice = db.Query(weather.All())
		.From(weather)
		.Where(warm | snowing)
		.Execute();

**Values are automatically parameterized**, including values passed to ```.In()```. This helps prevent an entire class of performance and security issues:

    var results = db.Query(posts.All())
        .From(posts)
        .Where(posts["title"] == "How to Make Money Slowly!")
        .Execute()
        .Select(post => post.title);

Joins
-----
    var posts = db["Posts"];
    var people = db["People"];

    var results = db.Query(people["name"], posts.All())
        .From(posts)
        .Join(people.On(people["id"] == posts["author"]))
        .OrderBy(people["name"].Desc(), posts["id"])
        .Execute()
        .Select(row => new { Author = row.name, Title = row.title });

... outer joins can be created by calling the ```.OuterJoin()``` method instead.

Unions
------
	// Does this make any sense? No, but you can do it!
	var easy = db.Query(Posts["title"].As("description"))
		.From(Posts)
		.Where(Posts["title"].Contains("Easy"));

	var leonardo = db.Query(People["name"].As("description"))
		.From(People)
		.Where(People["name"].Contains("Leonardo"));

	var results = easy.Union(leonardo)
		.Execute()
		.Select(row => row.description);

Subqueries
----------
You can write subqueries if you need to, but you should generally try not to:

	// Basically a join done with a subquery, which is a bad idea.
	var authors = db.Query(People["id"])
		.From(People)
		.Where(People["id"].In(1, 3));

	var results = db.Query(Posts.All())
		.From(Posts)
		.Where(Posts["author"].In(authors))
		.Execute();

	// ... and this one is done with a correlated subquery, which is even worse.
	results = db.Query(Posts.All())
		.From(Posts)
		.WhereExists(db.Query(People["id"])
			.From(People)
			.Where(People["id"] == Posts["author"])
			.Where(People["id"].In(1, 3))
		)
		.Execute();

Aggregates
----------
	var results = db.Query(Posts["author"], Sql.Func.Max(Posts["title"]), Sql.Func.Count().As("count"))
		.From(Posts)
		.GroupBy(Posts["author"])
		.Execute();

```Sql.Func``` implements a number of the relevant aggregate functions including:

	.Count()
	.Max(Expr)


Reusable Conditions
-------------------
You can decompose parts of queries into ```Func<Query, Query>``` transforms that can then easily be applied to queries allowing you to DRY concerns like applying permissions:

    Func<Query, Query> permissions = ...; 

    var results = db.Query(Posts.All())
        .From(Posts)
        .Apply(permissions)
        .Execute();

Limitations
-----------
* There's no easy way to define your schema so that you have strongly typed access to your tables and entites. We use T4 templates to generate our schema and entites. There should probably be a better way.
* You cannot Execute DDL. Writing SQL migration scripts and using something like [MigratorDotNet](https://code.google.com/p/migratordotnet/) is probably the way to go.
* It only works with SQL Server.

Props
-----
Obviously, this project wouldn't exist without [SQLAlchemy](http://www.sqlalchemy.org/), but a special mention also goes out to [Dapper](https://code.google.com/p/dapper-dot-net/) which is used to actually execute queries.
