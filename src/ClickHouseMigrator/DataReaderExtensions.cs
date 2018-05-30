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
						{
							value = reader.IsDBNull(i) ? 0 : Convert.ToInt32(value);
							break;
						}
					case "TINYINT":
						{
							value = reader.IsDBNull(i) ? Convert.ToByte(0) : Convert.ToByte(value);
							break;
						}
					case "BOOL":
						{
							value = reader.IsDBNull(i) ? Convert.ToByte(0) : Convert.ToByte(value);
							break;
						}
					case "SMALLINT":
						{
							value = reader.IsDBNull(i) ? Convert.ToInt16(0) : Convert.ToInt16(value);
							break;
						}
					case "BIGINT":
						{
							value = reader.IsDBNull(i) ? 0L : Convert.ToInt64(value);
							break;
						}
					case "FLOAT":
						{
							value = reader.IsDBNull(i) ? 0F : Convert.ToSingle(value);
							break;
						}
					case "DOUBLE":
						{
							value = reader.IsDBNull(i) ? 0.0 : Convert.ToDouble(value);
							break;
						}
					case "TIMESTAMP":
					case "DATE":
					case "DATETIME":
						{
							value = reader.IsDBNull(i) ? new DateTime() : Convert.ToDateTime(value);
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
