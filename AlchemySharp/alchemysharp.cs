/*
 * Copyright 2013, Fog Creek Software
 * License: http://www.apache.org/licenses/LICENSE-2.0 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Transactions;
using System.Web;
using AlchemySharp.Embed.Dapper;

namespace AlchemySharp {
    public class AlchemySharpException : Exception {
        public AlchemySharpException(string msg) : base(msg) {}
    }

    public class Case : Expr {
        private readonly Clause[] clauses;

        public Case(Clause[] clauses) {
            this.clauses = clauses;
        }

        public override string ToSQL(Parameters parameters) {
            var sb = new StringBuilder();

            sb.Append("case");

            foreach (var clause in clauses) {
                sb.Append(clause.ToSQL(parameters));
            }

            sb.AppendFormat("\nend");

            return sb.ToString();
        }
    }

    public abstract class Clause : Expr { }

    public class When : Clause {
        private readonly Expr condition;
        private readonly Expr value;

        public When(Expr condition, Expr value) {
            this.condition = condition;
            this.value = value;
        }

        public override string ToSQL(Parameters parameters) {
            return "\n\twhen {0} then {1}".Fmt(condition.ToSQL(parameters), value.ToSQL(parameters));
        }
    }

    public class Else : Clause { 
        private readonly Expr expr;

        public Else(Expr expr) {
            this.expr = expr;
        }

        public override string ToSQL(Parameters parameters) {
            return "\n\telse {0}".Fmt(expr.ToSQL(parameters));
        }
    }

    public class Sql {
        public static class Func {
            public static Selectable Cast(Expr expr, DataType dataType) {
                return new Function("CAST", new Cast(expr, dataType));
            }
            
            public static Selectable Count(object expr = null) {
                return new Function("COUNT", expr ?? new Raw("*"));
            }

            /// <summary>
            /// Returns the number of bytes used to represent any expression.
            /// </summary>
            public static Selectable DataLength(Expr expr) {
                return new Function("DATALENGTH", expr);
            }

            /// <summary>
            /// Returns part of a character, binary, text, or image expression.
            /// </summary>
            /// <param name="start">1-based index of where to start returning
            /// characters from.</param>
            /// <param name="length">How many characters to return. If start +
            /// length is greater than the number of characters in the expression,
            /// the entire expression beginning at start will be returned.</param>
            public static Selectable Substring(Expr expr, int start, int length) {
                return new Function("SUBSTRING", expr, start, length);
            }

            public static Selectable Max(Expr expr) {
                return new Function("MAX", expr);
            }
        }

        public static class DataTypes {
            public static DataType BigInt() {
                return new DataType(SqlDbType.BigInt);
            }

            public static DataType Binary(int? size = null) {
                return new DataType(SqlDbType.Binary, size);
            }

            public static DataType Bit() {
                return new DataType(SqlDbType.Bit);
            }

            public static DataType Char(int? size = null) {
                return new DataType(SqlDbType.Char, size);
            }

            public static DataType Date() {
                return new DataType(SqlDbType.Date);
            }

            public static DataType DateTime() {
                return new DataType(SqlDbType.DateTime);
            }

            public static DataType DateTime2(int? precision = null) {
                return new DataType(SqlDbType.DateTime2, precision);
            }

            public static DataType DateTimeOffset(int? precision = null) {
                return new DataType(SqlDbType.DateTimeOffset, precision);
            }

            public static DataType Decimal(int? precision = null, int? scale = null) {
                return new DataType(SqlDbType.Decimal, precision, scale);
            }

            public static DataType Float(int? size = null) {
                return new DataType(SqlDbType.Float, size);
            }

            public static DataType Image() {
                return new DataType(SqlDbType.Image);
            }

            public static DataType Int() {
                return new DataType(SqlDbType.Int);
            }

            public static DataType Money() {
                return new DataType(SqlDbType.Money);
            }

            public static DataType NChar(int? size = null) {
                return new DataType(SqlDbType.NChar, size);
            }

            public static DataType NText() {
                return new DataType(SqlDbType.NText);
            }

            public static DataType NVarChar(int? size = null) {
                return new DataType(SqlDbType.NVarChar, size);
            }

            public static DataType NVarCharMax() {
                return new DataType(SqlDbType.NVarChar, "MAX");
            }

            public static DataType Real() {
                return new DataType(SqlDbType.Real);
            }

            public static DataType SmallDateTime() {
                return new DataType(SqlDbType.SmallDateTime);
            }

            public static DataType SmallInt() {
                return new DataType(SqlDbType.SmallInt);
            }

            public static DataType SmallMoney() {
                return new DataType(SqlDbType.SmallMoney);
            }

            public static DataType Text() {
                return new DataType(SqlDbType.Text);
            }

            public static DataType Time(int? precision = null) {
                return new DataType(SqlDbType.Time, precision);
            }

            public static DataType Timestamp() {
                return new DataType(SqlDbType.Timestamp);
            }

            public static DataType TinyInt() {
                return new DataType(SqlDbType.TinyInt);
            }

            public static DataType UniqueIdentifier() {
                return new DataType(SqlDbType.UniqueIdentifier);
            }

            public static DataType VarBinary(int? size = null) {
                return new DataType(SqlDbType.VarBinary, size);
            }

            public static DataType VarBinaryMax() {
                return new DataType(SqlDbType.VarBinary, "MAX");
            }

            public static DataType VarChar(int? size = null) {
                return new DataType(SqlDbType.VarChar, size);
            }

            public static DataType VarCharMax() {
                return new DataType(SqlDbType.VarChar, "MAX");
            }

            public static DataType Variant() {
                return new DataType(SqlDbType.Variant);
            }
        }

        public static Case Case(params Clause[] clauses) {
            return new Case(clauses);
        }

        public static When When(object condition, object then) {
            return new When(condition.AsExpr(), then.AsExpr());
        }

        public static Else Else(object expr) {
            return new Else(expr.AsExpr());
        }

        public static Expr False() {
            return new BooleanExpr(false);
        }

        public static Expr True() {
            return new BooleanExpr(true);
        }

        public static Literal Literal(object obj) {
            return new Literal(obj);
        }

        internal static string Escape(string s) { 
            return s.Replace("\"", "\"\"");
        }
    }

    internal class Cast : Selectable {
        private readonly Expr expr;
        private readonly DataType dataType;

        public Cast(Expr expr, DataType dataType) {
            this.expr = expr;
            this.dataType = dataType;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0} AS {1}".Fmt(expr.ToSQL(parameters), dataType.ToSQL(parameters));
        }
    }

    internal class Function : Selectable {
        private readonly string name;
        private readonly IList<Expr> args;

        public Function(string name, params object[] args) {
            this.name = name;
            this.args = args.Select(arg => arg.AsExpr()).ToList();
        }

        public override string ToSQL(Parameters parameters) {
            return "{0}({1})".Fmt(name, string.Join(", ", args.Select(arg => arg.ToSQL(parameters))));
        }
    }

    public class DB : DynamicObject, IDisposable {
        private readonly DbConnection conn;

        private bool ownsConnection;
        internal DbConnection Connection { get { return conn; } }

        public TransactionScope BeginTransaction() {
            var scope = new TransactionScope();
            conn.EnlistTransaction(Transaction.Current);
            return scope;
        }

        public DB(DbConnection conn = null, bool ownsConnection = true) {
            this.conn = conn;
            this.ownsConnection = ownsConnection;
            if (conn != null && conn.State != ConnectionState.Open) {
                conn.Open();
            }

            Cache = new NoOpCache();
        }

        public Cache Cache { get; set; }

        public override bool TryGetMember(GetMemberBinder binder, out object result) {
            result = this[binder.Name];
            return true;
        }

        public Query Query(params Selectable[] cols) {
            return new Query(this)
                .Select(cols);
        }

        public virtual Update Update(Table t) {
            return new Update(this, t);
        }

        public virtual Insert Insert(Table t) {
            return new Insert(this, t);
        }

        public virtual Delete Delete(Table t) {
            return new Delete(this, t);
        }

        public void Dispose() {
            if (ownsConnection) {
                Connection.Dispose();
            }
        }

        public Table this[string key] {
            get {
                return new ConcreteTable(key);
            }
        }

        public Table DefineTable(string name, IEnumerable<string> columns) { 
            var table = this[name];
            return new DefinedTable(table, columns.Select(column => new Column(column, table)));
        }
    }

    public class Parameters {
        private readonly IDictionary<string, object> dict = new Dictionary<string, object>();
        
        internal DynamicParameters ForDapper { 
            get {
                var parameters = new DynamicParameters();
                foreach (var key in dict.Keys) {
                    parameters.Add(key, dict[key]);
                }
                return parameters;
            } 
        }

        private int id = 0;

        public string Next(object param) {
            var key = "@p" + id++;
            dict.Add(key, param);
            return key;
        }

        internal string Serialize() {
            var pairs = dict.Keys
                .OrderBy(name => name)
                .Select(name => string.Format("{0}={1}", name, dict[name]));

            return string.Join(",", pairs);
        }
    }

    public interface SQLNode {
        string ToSQL(Parameters parameters);
    }

    public class DataType : SQLNode {
        private readonly SqlDbType type;
        private readonly IList<object> args;

        public DataType(SqlDbType type, params object[] args) {
            this.type = type;
            this.args = args.Where(obj => obj != null).ToList();
        }

        public string ToSQL(Parameters parameters) {
            var sArgs = args.Count > 0 ? "({0})".Fmt(string.Join(", ", args)) : "";
            return "{0}{1}".Fmt(type.ToString(), sArgs);
        }
    }

    public interface Joinable : SQLNode {
        Table GetTable();
        Join On(object on);
        Join Join(Table right, object on = null);
        Join OuterJoin(Table right, object on = null);
    }

    public interface Join : Joinable { }

    public interface Table : Joinable {
        Table As(string alias);
        Column this[string name] { get; }
        IEnumerable<Column> Columns { get; }
        string Name { get; }
        Selectable All();
    }

    internal class ConcreteTable : ConcreteJoinable, Table {
        private readonly string name;
        private readonly string alias;

        public ConcreteTable(string name, string alias = null) {
            this.name = name;
            this.alias = alias;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result) {
            result = this[binder.Name];
            return true;
        }

        public virtual Column this[string key] {
            get {
                return new Column(key, this);
            }
        }

        public string Name { get { return name; } }

        public override string ToSQL(Parameters parameters) {
            var quoted = name.Quote();

            if (alias != null) {
                return "{0} as {1}".Fmt(quoted, alias.Quote());
            }

            return quoted;
        }

        public Table As(string alias) {
            return new ConcreteTable(name, alias);
        }

        public override Table GetTable() {
            return this;
        }

        public Selectable All() {
            return new All(this);
        }

        public IEnumerable<Column> Columns {
            get { throw new NotImplementedException(); }
        }
    }

    public class DelegatorTable : Table {
        protected readonly Table parent;

        public DelegatorTable(Table parent) {
            this.parent = parent;
        }

        public virtual Table As(string alias) {
            return parent.As(alias);
        }

        public virtual Column this[string name] {
            get { return parent[name]; }
        }

        public virtual IEnumerable<Column> Columns {
            get { return parent.Columns; }
        }

        public virtual string Name {
            get { return parent.Name; }
        }

        public virtual Table GetTable() {
            return parent.GetTable();
        }

        public virtual Join On(object on) {
            return parent.On(on);
        }

        public virtual Join Join(Table right, object on = null) {
            return parent.Join(right, on);
        }

        public virtual Join OuterJoin(Table right, object on = null) {
            return parent.OuterJoin(right, on);
        }

        public virtual string ToSQL(Parameters parameters) {
            return parent.ToSQL(parameters);
        }

        public virtual Selectable All() {
            return parent.All();
        }
    }

    internal class DefinedTable : DelegatorTable {
        private readonly List<Column> columns;

        public DefinedTable(Table parent, IEnumerable<Column> columns) : base(parent) {
            this.columns = new List<Column>(columns);
        }

        public override IEnumerable<Column> Columns {
            get {
                return columns.AsReadOnly();
            }
        }
    }

    public class All : Selectable {
        private readonly Table table;

        public All(Table table) {
            this.table = table;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0}.*".Fmt(table.ToSQL(parameters));
        }
    }

    public abstract class Selectable : Expr { }

    public class Alias : Selectable {
        private readonly string name;
        private readonly Expr expr;

        public Alias(Expr expr, string name) {
            this.expr = expr;
            this.name = name;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0} as {1}".Fmt(expr.ToSQL(parameters), name.Quote());
        }
    }

    public class Column : Selectable {
        private readonly string name;
        private readonly Table table;
        public string Name { get { return name; } }
        public Table Table { get { return table; } }

        public Column(string name, Table table) {
            this.name = name;
            this.table = table;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0}.{1}".Fmt(table.ToSQL(parameters), name.Quote());
        }
    }

    public abstract class Expr : SQLNode {
        public Expr Contains(string right) {
            return AlchemySharp.Like.Substring(this, right);
        }

        public Expr StartsWith(string right) {
            return AlchemySharp.Like.Prefix(this, right);
        }

        public Expr EndsWith(string right) {
            return AlchemySharp.Like.Suffix(this, right);
        }

        public Expr Like(string like) {
            return new Like(this, like);
        }

        public Expr IsNull() {
            return new IsNull(this);
        }

        public Expr IsNotNull() {
            return ~this.IsNull();
        }

        public Expr In(Query query) {
            return new BinaryExpr(this, new Parens(query), "in");
        }

        public Expr In(IEnumerable<object> values) {
            return new BinaryExpr(this, new ParensList(values), "in");
        }

        public Expr In(params object[] values) {
            return this.In((IEnumerable<object>)values);
        }

        public Selectable As(string name) {
            return new Alias(this, name);
        }

        public Expr Desc(bool descending = true) {
            if (descending) {
                return new Desc(this);
            }

            return this;
        }

        public static Expr operator |(Expr left, Expr right) {
            return new BinaryExpr(left, right, "or");
        }

        public static Expr operator &(Expr left, Expr right) {
            return new BinaryExpr(left, right, "and");
        }

        public static Expr operator >(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), ">");
        }

        public static Expr operator <(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "<");
        }

        public static Expr operator <=(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "<=");
        }

        public static Expr operator >=(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), ">=");
        }

        public static Expr operator ==(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "="); ;
        }

        public static Expr operator !=(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "<>"); ;
        }

        public static Expr operator ~(Expr self) {
            return new UnaryExpr("not", self);
        }

        public static Expr operator +(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "+");
        }

        public static Expr operator -(Expr left, object right) {
            return new BinaryExpr(left, right.AsExpr(), "-");
        }

        public override bool Equals(object obj) {
            return base.Equals(obj);
        }

        public override int GetHashCode() {
            return base.GetHashCode();
        }

        public abstract string ToSQL(Parameters parameters);
    }

    internal class BooleanExpr : Expr {
        private readonly bool value;
        public BooleanExpr(bool value) {
            this.value = value;
        }

        public override string ToSQL(Parameters parameters) {
            return "1 = {0}".Fmt(value ? 1 : 0);
        }
    }

    internal class IsNull : Expr {
        private readonly Expr expr;

        public IsNull(Expr expr) {
            this.expr = expr;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0} is NULL".Fmt(expr.ToSQL(parameters));
        }
    }

    internal class Exists : Expr {
        private readonly Query query;

        public Exists(Query query) {
            this.query = query;
        }

        public override string ToSQL(Parameters parameters) {
            return "exists ({0})".Fmt(query.ToSQL(parameters));
        }
    }

    internal class Desc : Expr {
        private readonly Expr expr;

        public Desc(Expr expr) {
            this.expr = expr;
        }

        public override string ToSQL(Parameters parameters) {
            return "{0} desc".Fmt(expr.ToSQL(parameters));
        }
    }
     
    public class Literal : Selectable {
        private readonly object obj;

        public object Value { get { return obj; } }

        public Literal(object obj) {
            this.obj = obj;
        }

        public override string ToSQL(Parameters parameters) {
            return parameters.Next(obj);
        }
    }

    internal class Raw : Expr {
        private readonly string raw;

        public Raw(string raw) {
            this.raw = raw;
        }

        public override string ToSQL(Parameters parameters) {
            return raw;
        }
    }

    internal class Parens : Expr {
        private readonly Expr expr;

        public Parens(Expr expr) {
            this.expr = expr;
        }

        public override string ToSQL(Parameters parameters) {
            return "({0})".Fmt(expr.ToSQL(parameters));
        }
    }

    internal class ParensList : Expr {
        IEnumerable<Expr> values;
        public ParensList(IEnumerable<object> values) {
            var valueList = values.ToList();
            if (valueList.Count > 20) {
                throw new ArgumentException("Too many values.");
            }
            this.values = valueList.Select(value => value.AsExpr());
        }

        public override string ToSQL(Parameters parameters) {
            return "({0})".Fmt(", ".Join(values.Select(value => value.ToSQL(parameters))));
        }

    }

    internal class BinaryExpr : Expr {
        private readonly Expr left;
        private readonly Expr right;
        private readonly string op;

        public BinaryExpr(Expr left, Expr right, string op) {
            this.left = left;
            this.right = right;
            this.op = op;
        }

        public override string ToSQL(Parameters parameters) {
            return "({0} {1} {2})".Fmt(left.ToSQL(parameters), op, right.ToSQL(parameters));
        }
    }

    internal class UnaryExpr : Expr {
        private readonly Expr self;
        private readonly string op;

        public UnaryExpr(string op, Expr self) {
            this.op = op;
            this.self = self;
        }

        public override string ToSQL(Parameters parameters) {
            return "({0} {1})".Fmt(op, self.ToSQL(parameters));
        }
    }

    internal class Like : BinaryExpr {
        public Like(Expr left, string right) 
            : base(left, new Literal(right), "like") {}

        private static string Escape(string s) {
            var sb = new StringBuilder();
            foreach (var c in s) {
                switch (c) {
                    case '%':
                    case '_':
                    case '[':
                        sb.Append("[" + c + "]");
                        break;
                    default:
                        sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
        }

        internal static Like Prefix(Expr left, string right) {
            return new Like(left, Escape(right) + "%");
        }

        internal static Like Suffix(Expr left, string right) {
            return new Like(left, "%" + Escape(right));
        }

        internal static Like Substring(Expr left, string right) {
            return new Like(left, "%" + Escape(right) + "%");
        }
    }

    internal abstract class ConcreteJoinable : DynamicObject, Joinable { 
        public Join On(object on) {
            var condition = on as Expr ?? new Literal((string)on);
            return new ConcreteJoin(this, condition);
        }

        public Join Join(Table right, object on = null) {
            return Join(right, on, outer: false);
        }

        public Join OuterJoin(Table right, object on = null) {
            return Join(right, on, outer: true);
        }

        protected Join Join(Table right, object on, bool outer) {
            var condition = Rewrite(on as Expr ?? new Literal((string)on), GetTable(), right);
            return new ConcreteJoin(right, condition, this, outer);
        }

        protected Expr Rewrite(Expr condition, Table left, Table right) {
            var literal = condition as Literal;

            if (!object.Equals(literal, null) && left != null && right != null) {
                var key = literal.Value as String;
                return left[key] == (right[key]);
            }

            return condition;
        }

        public abstract Table GetTable();
        public abstract string ToSQL(Parameters parameters);
    }

    internal class ConcreteJoin : ConcreteJoinable, Join {
        private readonly Joinable left;
        private readonly Joinable right;
        private readonly bool outer;
        private readonly Expr condition;

        public ConcreteJoin(Joinable right, Expr condition, Joinable left = null, bool outer = false) {
            this.left = left;
            this.right = right;
            this.outer = outer;
            this.condition = condition;
        }

        internal Join Left(Joinable left, Table root) {
            return new ConcreteJoin(right, Rewrite(condition, root, right.GetTable()), left, outer);
        }

        internal Join Outer() {
            return new ConcreteJoin(right, condition, left, outer: true);
        }

        public override string ToSQL(Parameters parameters) {
            return "({0} \n\t{1}join {2} on {3})".Fmt(left.ToSQL(parameters), (outer ? "left " : ""), right.ToSQL(parameters), condition.ToSQL(parameters));
        }

        public override Table GetTable() {
            return right.GetTable() ;
        }
    }

    public interface Cache {
        object this[string key] { get; set; }
    }

    internal class NoOpCache : Cache{
        public object this[string key] {
            get { return null; }
            set {}
        }
    }

    public class Query : Expr {
        private int limit = 0;
        private readonly IList<Selectable> columns = new List<Selectable>();
        private Joinable from;
        private readonly IList<Join> joins = new List<Join>();
        private readonly IList<Expr> conditions = new List<Expr>();
        private readonly IList<Expr> groupBy = new List<Expr>();
        private readonly IList<Expr> orderBy = new List<Expr>();
        private readonly DB db;
        private Table root;
        private readonly IList<Query> unions = new List<Query>();

        public Table Root { get { return root; }  }

        public Query(DB db) {
            this.db = db;
        }

        public IEnumerable<dynamic> Execute() {
            var parameters = new Parameters();
            var sql = ToSQL(parameters);
            var key = string.Format("query:{0}:{1}", sql, parameters.Serialize());
            return db.Cache.Get(key, () => db.Connection.Query(sql, parameters.ForDapper));
        }

        public Query From(Joinable from) {
            if (root == null) {
                root = from as Table;
            }

            this.from = from;
            return this;
        }

        public Query Join(params Join[] joins) {
            foreach (var join in joins.Cast<ConcreteJoin>()) {
                from = join.Left(from, root);
            }

            return this;
        }

        public Query OuterJoin(params Join[] joins) {
            return Join(joins.Cast<ConcreteJoin>().Select(join => join.Outer()).ToArray());
        }

        public Query Select(params Selectable[] columns) {
            this.columns.AddAll(columns);
            return this;
        }

        public Query Where(params Expr[] conditions) {
            this.conditions.AddAll(conditions);
            return this;
        }

        public Query WhereExists(Query query) {
            this.conditions.Add(new Exists(query));
            return this;
        }

        public Query Limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Query GroupBy(params Expr[] groupBy) {
            this.groupBy.AddAll(groupBy);
            return this;
        }

        public Query OrderBy(params Expr[] orderBy) {
            this.orderBy.AddAll(orderBy);
            return this;
        }

        public Query Apply(Func<Query, Query> tranform) {
            return tranform(this);
        }

        public Query Clone() {
            var clone = new Query(db) { 
                from = from,
                limit = limit,
                root = root,
            };

            clone.columns.AddAll(columns);
            clone.joins.AddAll(joins);
            clone.conditions.AddAll(conditions);
            clone.orderBy.AddAll(orderBy);
            clone.groupBy.AddAll(groupBy);
            clone.unions.AddAll(unions.Select(query => query.Clone()));

            return clone;
        }


        public Query Union(Query other) {
            unions.Add(other);
            return this;
        }

        public string ToSQL() {
            return ToSQL(new Parameters());
        }

        public override string ToSQL(Parameters parameters) {
            var sb = new StringBuilder();

            if (from == null) {
                throw new AlchemySharpException("MissingFromClause"); 
            }

            sb.AppendFormat(
                "(select {0} {1}\nfrom {2}",
                limit > 0 ? "top " + limit : "", 
                ", ".Join(columns.Select(c => c.ToSQL(parameters))),
                from.ToSQL(parameters)
            );

            if (conditions.Count > 0) {
                sb.AppendFormat("\nwhere {0}", " and ".Join(conditions.Select(cond => cond.ToSQL(parameters))));
            }

            foreach (var union in unions) {
                sb.AppendFormat("\nunion {0}", union.ToSQL(parameters));
            }

            if (groupBy.Count > 0) {
                sb.AppendFormat("\ngroup by {0}", ", ".Join(groupBy.Select(expr => expr.ToSQL(parameters))));
            }

            sb.Append("\n)"); // in the T-SQL grammar this paren occurs before the order by

            if (orderBy.Count > 0){
                sb.AppendFormat("\norder by {0}", ", ".Join(orderBy.Select(expr => expr.ToSQL(parameters))));
            }

            return sb.ToString();
        }
    }

    public interface IModifyQuery {
        IModifyQuery Set(Column c, object e);
        void Execute();
        int? JustInsertedIx { get; }
    }

    public class Update : IModifyQuery {
        private readonly DB db;
        protected readonly Table table;
        private readonly IList<Expr> conditions = new List<Expr>();
        private readonly IList<Tuple<Column,Expr>> sets = new List<Tuple<Column,Expr>>();

        public Update(DB db, Table table) {
            this.db = db;
            this.table = table;
        }

        public int? JustInsertedIx {
            get { return null; }
        }

        public virtual void Execute() {
            var parameters = new Parameters();
            var sql = ToSQL(parameters);
            db.Connection.Execute(sql, parameters.ForDapper);
        }

        IModifyQuery IModifyQuery.Set(Column c, object e) {
            return Set(c, e);
        }

        public Update Set(Column c, object e) {
            this.sets.Add(new Tuple<Column, Expr>(c, e.AsExpr()));
            return this;
        }

        public Update Where(params Expr[] conditions) {
            this.conditions.AddAll(conditions);
            return this;
        }

        public string ToSQL(Parameters parameters) {
            var sb = new StringBuilder();

            sb.AppendFormat(
                "update {0}\nset {1}",
                table.ToSQL(parameters),
                ", ".Join(sets.Select(s => "{0} = {1}".Fmt(s.Item1.ToSQL(parameters), s.Item2.ToSQL(parameters))))
            );

            if (conditions.Count > 0) {
                sb.AppendFormat("\nwhere {0}", " and ".Join(conditions.Select(cond => cond.ToSQL(parameters))));
            }

            return sb.ToString();
        }

    }

    public class Insert : IModifyQuery {
        private readonly DB db;
        protected readonly Table table;
        private readonly IList<Tuple<Column, Expr>> sets = new List<Tuple<Column, Expr>>();

        private int? ixInserted;

        public int? JustInsertedIx {
            get { return ixInserted; }
        }

        public Insert(DB db, Table table) {
            this.db = db;
            this.table = table;
        }

        public virtual void Execute() {
            var parameters = new Parameters();
            var sql = ToSQL(parameters);
            var result = db.Connection.Query(sql, parameters.ForDapper);

            ixInserted = (int)result.First().ix;
        }

        IModifyQuery IModifyQuery.Set(Column c, object e) {
            return Set(c, e);
        }

        public Insert Set(Column c, object e) {
            this.sets.Add(new Tuple<Column, Expr>(c, e.AsExpr()));
            return this;
        }

        public string ToSQL(Parameters parameters) {
            var sb = new StringBuilder();

            sb.AppendFormat(
                "insert into {0}\n ({1}) \nVALUES ({2});",
                table.ToSQL(parameters),
                ", ".Join(sets.Select(s => s.Item1.ToSQL(parameters))),
                ", ".Join(sets.Select(s => s.Item2.ToSQL(parameters)))
            );

            sb.Append("SELECT @@IDENTITY AS ix;");

            return sb.ToString();
        }
    }

    public class Delete {
        private readonly DB db;
        protected readonly Table table;
        private readonly IList<Expr> conditions = new List<Expr>();

        public Delete(DB db, Table table) {
            this.db = db;
            this.table = table;
        }

        public virtual int Execute() {
            var parameters = new Parameters();
            var sql = ToSQL(parameters);
            return db.Connection.Execute(sql, parameters.ForDapper);
        }

        public Delete Where(params Expr[] conditions) {
            this.conditions.AddAll(conditions);
            return this;
        }

        public string ToSQL(Parameters parameters) {
            var sb = new StringBuilder();

            sb.AppendFormat(
                "delete from {0}\n",
                table.ToSQL(parameters)
            );

            if (conditions.Count > 0) {
                sb.AppendFormat("\nwhere {0}", " and ".Join(conditions.Select(cond => cond.ToSQL(parameters))));
            }

            return sb.ToString();
        }
    }

    static class Extensions {
        public static void AddAll<T>(this IList<T> list, IEnumerable<T> toAdd) {
            foreach (var x in toAdd) {
                list.Add(x);
            }
        }

        public static string Fmt(this string tmpl, params object[] args) {
            return String.Format(tmpl, args);
        }

        public static string Join(this string delim, IEnumerable<string> bits) {
            return String.Join(delim, bits);
        }

        public static Expr AsExpr(this object obj) {
            return obj as Expr ?? new Literal(obj);
        }

        public static string Quote(this string s) {
            var escaped = s.Replace("\"", "\"\"");
            return "\"{0}\"".Fmt(escaped);
        }

        public static T Get<T>(this Cache cache, string key, Func<T> f) where T : class {
            var result = cache[key];
            if (result == null) {
                cache[key] = result = f();
            }

            return (T)result;
        }
    }
}