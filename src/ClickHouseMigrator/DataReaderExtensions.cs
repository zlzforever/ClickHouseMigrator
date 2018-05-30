using System;
using System.Data;

namespace ClickHouseMigrator
{
	public static class DataReaderExtensions
	{
		private static readonly DateTime Today = DateTime.Now.Date;

		public static dynamic[] ToArray(this IDataReader reader)
		{
			dynamic[] result = new dynamic[reader.FieldCount + 1];
			result[reader.FieldCount] = Today;

			for (int i = 0; i < reader.FieldCount; ++i)
			{
				var value = reader.GetValue(i);
				var dbtype = reader.GetDataTypeName(i);

				switch (dbtype)
				{
					case "INT":
					case "TINYINT":
					case "BOOL":
						{
							value = reader.IsDBNull(i) ? -1 : Convert.ToInt32(value);
							break;
						}
					case "BIGINT":
						{
							value = reader.IsDBNull(i) ? 0 : Convert.ToInt64(value);
							break;
						}
					case "FLOAT":
						{
							value = reader.IsDBNull(i) ? Single.NaN : Convert.ToSingle(value);
							break;
						}
					case "DOUBLE":
						{
							value = reader.IsDBNull(i) ? 0 : Convert.ToDouble(value);
							break;
						}
					case "TIMESTAMP":
					case "DATE":
					case "DATETIME":
						{
							value = reader.IsDBNull(i) ? new DateTime(1970, 1, 1, 0, 0, 0) : Convert.ToDateTime(value);
							break;
						}
					default:
						{
							value = reader.IsDBNull(i) ? string.Empty : Convert.ToString(value);
							break;
						}
				}
				result[i] = value;
			}

			return result;
		}
	}
}
