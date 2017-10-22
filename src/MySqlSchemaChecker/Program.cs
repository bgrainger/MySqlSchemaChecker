extern alias MySqlData;
extern alias MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Markup;
using OldConnection = MySqlData.MySql.Data.MySqlClient.MySqlConnection;
using NewConnection = MySqlConnector.MySql.Data.MySqlClient.MySqlConnection;
using OldDbType = MySqlData.MySql.Data.MySqlClient.MySqlDbType;
using NewDbType = MySqlConnector.MySql.Data.MySqlClient.MySqlDbType;

namespace MySqlSchemaChecker
{
	class Program
	{
		static void Main()
		{
			var connectionString = "server=localhost;user id=mysqltest;password='test;key=\"val';port=3306;database=mysqltest;ssl mode=none;Use Affected Rows=true";

			CompareDbTypes();

			using (var oldConnection = new OldConnection(connectionString))
			using (var newConnection = new NewConnection(connectionString))
			{
				oldConnection.Open();
				newConnection.Open();

				CompareDataTypes(oldConnection, newConnection);
			}
		}

		static void CompareDbTypes()
		{
			foreach (OldDbType value in Enum.GetValues(typeof(OldDbType)))
			{
				if (((NewDbType) (int) value).ToString() != value.ToString())
					Console.WriteLine($"{value.ToString()} (old) != {((NewDbType) (int) value).ToString()} (new)");
			}
		}

		static void CompareDataTypes(OldConnection oldConnection, NewConnection newConnection)
		{
			var oldDataTypes = oldConnection.GetSchema("DataTypes");
			var newDataTypes = newConnection.GetSchema("DataTypes");

			Console.WriteLine("Loading old data types");
			var (oldRows, oldExtra) = GetRowsByType(oldDataTypes);
			Console.WriteLine("Loading new data types");
			var (newRows, newExtra) = GetRowsByType(newDataTypes);
			Console.WriteLine();

			Console.WriteLine("## Same Types in Both Connectors");
			Console.WriteLine();
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => x.ColumnName)) + " |");
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => "---")) + " |");
			foreach (var oldPair in oldRows)
			{
				if (newRows.TryGetValue(oldPair.Key, out var newRow))
				{
					var oldValues = oldPair.Value.ItemArray;
					var newValues = newRow.ItemArray;
					for (int i = 0; i < oldValues.Length; i++)
					{
						var equal = oldValues[i].Equals(newValues[i]);
						Console.Write("| {0}{1}{0} ", equal ? "" : "**", ValueToMarkdown(newValues[i]));
					}
					Console.WriteLine("|");
				}
			}
			Console.WriteLine();

			Console.WriteLine("## Only in MySql.Data");
			Console.WriteLine();
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => x.ColumnName)) + " |");
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => "---")) + " |");
			foreach (var oldRow in oldRows.Where(x => !newRows.ContainsKey(x.Key)).Select(x => x.Value).Concat(oldExtra))
			{
				var oldValues = oldRow.ItemArray;
				for (int i = 0; i < oldValues.Length; i++)
					Console.Write("| {0} ", ValueToMarkdown(oldValues[i]));
				Console.WriteLine("|");
			}
			Console.WriteLine();

			Console.WriteLine("## Only in MySqlConnector");
			Console.WriteLine();
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => x.ColumnName)) + " |");
			Console.WriteLine("| " + string.Join(" | ", oldDataTypes.Columns.Cast<DataColumn>().Select(x => "---")) + " |");
			foreach (var newRow in newRows.Where(x => !oldRows.ContainsKey(x.Key)).Select(x => x.Value).Concat(newExtra))
			{
				var newValues = newRow.ItemArray;
				for (int i = 0; i < newValues.Length; i++)
					Console.Write("| {0} ", ValueToMarkdown(newValues[i]));
				Console.WriteLine("|");
			}
			Console.WriteLine();
		}

		static (Dictionary<int, DataRow> Keyed, List<DataRow> Extra) GetRowsByType(DataTable dataTable)
		{
			var keyed = new Dictionary<int, DataRow>();
			var extra = new List<DataRow>();
			foreach (DataRow row in dataTable.Rows)
			{
				var dbType = row.Field<int>("ProviderDbType");
				if (!keyed.ContainsKey(dbType))
					keyed.Add(dbType, row);
				else
					extra.Add(row);
			}
			return (keyed, extra);
		}

		static string ValueToMarkdown(object value) => value == null || DBNull.Value.Equals(value) ? "_null_" : value.ToString();
	}
}
