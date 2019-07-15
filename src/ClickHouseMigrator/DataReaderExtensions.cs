using System;
using System.Data;

namespace ClickHouseMigrator
{
	public static class DataReaderExtensions
	{
		public static dynamic[] ToArray(this IDataReader reader)
		{
			dynamic[] result = new dynamic[reader.FieldCount];
			for (int i = 0; i < reader.FieldCount; ++i)
			{
				var value = reader.GetValue(i);
				var dbType = reader.GetDataTypeName(i);				
				switch (dbType.ToLowerInvariant())
				{
					case "int":					
						{
							value = reader.IsDBNull(i) ? 0 : Convert.ToInt32(value);
							break;
						}
					case "tinyint":
						{
							value = reader.IsDBNull(i) ? Convert.ToByte(0) : Convert.ToByte(value);
							break;
						}
					case "bool":
						{
							value = reader.IsDBNull(i) ? Convert.ToByte(0) : Convert.ToByte(value);
							break;
						}
					case "smallint":
						{
							value = reader.IsDBNull(i) ? Convert.ToInt16(0) : Convert.ToInt16(value);
							break;
						}
					case "bigint":
						{
							value = reader.IsDBNull(i) ? 0L : Convert.ToInt64(value);
							break;
						}
					case "float":
						{
							value = reader.IsDBNull(i) ? 0F : Convert.ToSingle(value);
							break;
						}
					case "double":
					case "decimal":					
						{
							value = reader.IsDBNull(i) ? 0.0 : Convert.ToDouble(value);
							break;
						}
					case "timestamp":
					case "date":
					case "datetime":
					case "datetime2":
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
