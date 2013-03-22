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


Parameterized Queries
---------------------
Queries are automatically paramerized, preventing an entire class of performance and security issues:

    var results = db.Query(posts.All())
        .From(posts)
        .Where(posts["title"] == "How to Make Money Slowly!")
        .Execute()
        .Select(post => post.title);


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
