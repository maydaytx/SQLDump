using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using NDesk.Options;

namespace SQLDump
{
	internal static class Program
	{
		private static int Main(string[] args)
		{
			var options = new Options();

			var optionSet = new OptionSet
			{
			    {"i|use-integrated-security", "use Integrated Security to connect to server (default)", x => {}},
			    {"s|use-sql-server-authentication", "use SQL Server authentication to connect to server", x => options.UseSqlServerAuthenication = x != null},
			    {"u|username=", "username for SQL Server authentication", x => options.Username = x},
			    {"p|password=", "password for SQL Server authentication", x => options.Password = x},
			    {"l|limit=", "limit number of records per table", x => options.Limit = int.Parse(x)},
			    {"t|use-transaction", "wrap all insert statements in a transaction", x => options.UseTransaction = x != null},
			    {"d|identity-insert", "include statement to enable identity insert and include identity column in output", x => options.IncludeIdentityInsert = x != null},
			    {"e|exclude", "supplied tables are excluded, rather than included", x => options.ListIsExclusive = x != null},
			    {"?|help", "display this help and exit", x => options.ShowHelp = x != null},
			    {"version", "output version information then exit", x => options.ShowVersion = x != null},
			};

			IList<string> arguments;

			try
			{
				arguments = optionSet.Parse(args);
			}
			catch (Exception ex)
			{
				PrintError(ex.ToString());
				return 1;
			}

			if (options.ShowHelp)
			{
				PrintHelp(optionSet);
				return 0;
			}
			else if (options.ShowVersion)
			{
				PrintVersion();
				return 0;
			}
			else if (arguments.Count < 2)
			{
				PrintError("Not enough arguments supplied");
				return 1;
			}
			else if (options.UseSqlServerAuthenication && (options.Username == null || options.Password == null))
			{
				PrintError("Must supply username and password for SQL Server Authentication");
				return 1;
			}

			options.Server = arguments[0];
			options.Database = arguments[1];
			options.TableNames = arguments.Skip(2).ToList();

			try
			{
				PerformDump(options);
			}
			catch (Exception ex)
			{
				PrintError(ex.ToString());
				return 1;
			}

			return 0;
		}

		private static void PerformDump(Options options)
		{
			var connectionString = GetConnectionString(options.Server, options.Database, options.UseSqlServerAuthenication, options.Username, options.Password);

			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				var tablesToDump = GetTablesToDump(connection, options.TableNames, options.ListIsExclusive);

				if (options.UseTransaction)
				{
					Console.WriteLine("begin transaction");
					Console.WriteLine();
				}

				var first = true;
				foreach (var table in tablesToDump)
				{
					if (first)
						first = false;
					else
						Console.WriteLine();

					DumpTable(connection, table, options.IncludeIdentityInsert, options.Limit);
				}

				if (options.UseTransaction)
				{
					Console.WriteLine();
					Console.WriteLine("commit transaction");
				}
			}
		}

		private static string GetConnectionString(string server, string database, bool useSqlServerAuthenication, string username, string password)
		{
			if (useSqlServerAuthenication)
			{
				return string.Format("Data Source={0};Initial Catalog={1};User Id={2};Password={3};", server, database, username, password);
			}
			else
			{
				return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=SSPI;Trusted_Connection=yes;", server, database);
			}
		}

		private static IEnumerable<TableInfo> GetTablesToDump(IDbConnection connection, ICollection<string> tableNames, bool listIsExclusive)
		{
			const string sqlFormat =
@"select
	t.table_name,
	(select top 1
		c.column_name
	from
		information_schema.columns c
	where
		c.table_schema = 'dbo'
		and c.table_name = t.table_name
		and columnproperty(object_id(c.table_name), c.column_name, 'IsIdentity') = 1
	) as identity_column
from
	information_schema.tables t
where
	t.table_schema = 'dbo'
	and t.table_type = 'BASE TABLE'{0}
order by
	t.table_name";

			string sql;

			if (tableNames.Count == 0)
			{
				sql = string.Format(sqlFormat, string.Empty);
			}
			else if (listIsExclusive)
			{
				sql = string.Format(sqlFormat, "\n\tand t.table_name not in ('" + string.Join("', '", tableNames) + "')");
			}
			else
			{
				sql = string.Format(sqlFormat, "\n\tand t.table_name in ('" + string.Join("', '", tableNames) + "')");
			}

			var tableList = new List<TableInfo>();

			using (var command = connection.CreateCommand())
			{
				command.CommandText = sql;

				using (var reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						var tableName = reader.GetString(0);
						var identityColumn = reader.GetString(1);

						tableList.Add(new TableInfo {Name = tableName, IdentityColumn = identityColumn});
					}
				}
			}

			return tableList;
		}

		private static void DumpTable(IDbConnection connection, TableInfo table, bool includeIdentityInsert, int? limit)
		{
			if (includeIdentityInsert)
			{
				Console.WriteLine("set identity_insert [" + table.Name + "] on");
				Console.WriteLine();
			}

			using (var command = connection.CreateCommand())
			{
				if (limit != null)
					command.CommandText = "select top " + limit + " * from [" + table.Name + "]";
				else
					command.CommandText = "select * from [" + table.Name + "]";

				using (var reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						DumpRow(table, reader, includeIdentityInsert);
					}
				}
			}

			if (includeIdentityInsert)
			{
				Console.WriteLine();
				Console.WriteLine("set identity_insert [" + table + "] off");
			}
		}

		private static void DumpRow(TableInfo table, IDataRecord reader, bool includeIdentityInsert)
		{
			Console.Write("insert into [" + table.Name + "] (");

			var first = true;
			for (var i = 0; i < reader.FieldCount; ++i)
			{
				var columnName = reader.GetName(i);

				if (includeIdentityInsert || columnName != table.IdentityColumn)
				{
					if (first)
						first = false;
					else
						Console.Write(", ");

					Console.Write("[" + columnName + "]");
				}
			}

			Console.Write(") values (");

			first = true;
			for (var i = 0; i < reader.FieldCount; ++i)
			{
				var columnName = reader.GetName(i);

				if (includeIdentityInsert || columnName != table.IdentityColumn)
				{
					if (first)
						first = false;
					else
						Console.Write(", ");

					var literal = ConvertToSqlLiteral(reader.GetFieldType(i), reader.GetValue(i));

					Console.Write(literal);
				}
			}

			Console.WriteLine(")");
		}

		private static string ConvertToSqlLiteral(Type type, object value)
		{
			if (value == DBNull.Value)
			{
				return "null";
			}
			else if (type == typeof (string))
			{
				return "'" + ((string) value).Replace("'", "''") + "'";
			}
			else if (type == typeof (DateTime))
			{
				return "'" + ((DateTime) value).ToString("yyyy-MM-dd hh:mm:ss.fff") + "'";
			}
			else if (type == typeof (byte[]))
			{
				return GetHexString((byte[]) value);
			}
			else if (type == typeof (Guid))
			{
				return "'" + ((Guid) value).ToString("D") + "'";
			}
			else if (type == typeof (bool))
			{
				return ((bool) value) ? "1" : "0";
			}
			else
			{
				return value.ToString();
			}
		}

		private static string GetHexString(IEnumerable<byte> value)
		{
			var sb = new StringBuilder("'0x");

			foreach (var @byte in value)
			{
				sb.AppendFormat("{0:x2}", @byte);
			}

			sb.Append("'");

			return sb.ToString();
		}

		private static void PrintError(string message)
		{
			var originalColor = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Red;

			Console.Error.WriteLine("ERROR: " + message);

			Console.ForegroundColor = originalColor;
		}

		private static void PrintHelp(OptionSet optionSet)
		{
			PrintVersion();

			var assemblyName = Path.GetFileNameWithoutExtension(Process.GetCurrentProcess().MainModule.FileName);

			Console.WriteLine();
			Console.WriteLine("Usage: " + assemblyName + " [OPTIONS] SERVER DATABASE [TABLES]");
			Console.WriteLine();
			Console.WriteLine("Options:");

			optionSet.WriteOptionDescriptions(Console.Out);
		}

		private static void PrintVersion()
		{
			var version = Assembly.GetEntryAssembly().GetName().Version.ToString();

			if (version.Length % 2 == 1)
				version = string.Format(" SQLDump  {0} ", version);
			else
				version = string.Format(" SQLDump {0} ", version);

			var padding = new string(Enumerable.Repeat('8', (26 - version.Length)/2).ToArray());

			var versionWithPadding = padding + version + padding;

			Console.WriteLine();
			Console.WriteLine(
@"                        (                      
                         )                     
                    (   (                      
                     )   b                     
                    (    88_                   
                      ___888b__                
                    _d888888888b   (           
           (    ___d888888888888_   )          
            )  d88888888888888888b (           
           (  d8888888888888888888__           
           ___8888888888888888888888b          
          d{0}b         
          888888888888888888888888888P         
          Y8888888888888888888888888P          ", versionWithPadding);
		}

		private class Options
		{
			public string Server { get; set; }
			public string Database { get; set; }
			public bool UseSqlServerAuthenication { get; set; }
			public string Username { get; set; }
			public string Password { get; set; }
			public int? Limit { get; set; }
			public bool UseTransaction { get; set; }
			public bool IncludeIdentityInsert { get; set; }
			public bool ListIsExclusive { get; set; }
			public bool ShowHelp { get; set; }
			public bool ShowVersion { get; set; }
			public IList<string> TableNames { get; set; } 
		}

		private class TableInfo
		{
			public string Name { get; set; }
			public string IdentityColumn { get; set; }
		}
	}
}
