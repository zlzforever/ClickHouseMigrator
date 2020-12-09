using System;
using System.Data;
using Dapper;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator
{
	// ReSharper disable once InconsistentNaming
	public abstract class RDBMSMigrator : Migrator
	{
		private Lazy<IDataReader> _reader;

		protected abstract Lazy<string> ConnectionString { get; }

		protected override (dynamic[][] Data, int Length) FetchRows(int count)
		{
			var list = new dynamic[count][];
			var i = 0;

			while (_reader.Value.Read())
			{
				dynamic[] data = new dynamic[_reader.Value.FieldCount];
				_reader.Value.GetValues(data);
				list[i] = data;
				i++;
				if (i >= count)
				{
					break;
				}
			}

			return (list, i);
		}

		protected override void Initialize(IConfiguration configuration)
		{
			_reader = new Lazy<IDataReader>(() =>
			{
				var conn = CreateDbConnection();
				return conn.ExecuteReader(GetSelectAllSql());
			});
		}

		protected override void InitializeTable()
		{
			foreach (var column in Columns.Value)
			{
				column.DataType = ConvertToClickHouseDataType(column.DataType);
			}

			base.InitializeTable();
		}

		protected virtual string ConvertToClickHouseDataType(string type)
		{
			var sizePrefixIndex = type.IndexOf('(');
			var normalTypeName = sizePrefixIndex <= 0 ? type : type.Substring(0, sizePrefixIndex);
			switch (normalTypeName.ToLower())
			{
				case "timestamp":
				case "datetime2":
				case "datetime":
				{
					return "DateTime";
				}
				case "date":
				{
					return "Date";
				}
				case "tinyint":
				{
					return "UInt8";
				}
				case "smallint":
				{
					return "Int16";
				}
				case "int":
				{
					return "Int32";
				}
				case "float":
				{
					return "Float32";
				}
				case "double":
				case "decimal":
				{
					return "Float64";
				}
				case "bigint":
				{
					return "Int64";
				}
				default:
				{
					return "String";
				}
			}
		}

		protected abstract IDbConnection CreateDbConnection();

		protected abstract string GetSelectAllSql();
	}
}