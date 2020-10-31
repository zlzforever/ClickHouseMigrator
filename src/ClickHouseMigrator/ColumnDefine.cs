namespace ClickHouseMigrator
{
	public class ColumnDefine
	{
		public string Name { get; set; }
		public string DataType { get; set; }
		public int Index { get; set; }
		public bool IsPrimary { get; set; }
	}
}
