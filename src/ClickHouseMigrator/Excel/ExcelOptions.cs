using System;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator.Excel
{
	public class ExcelOptions : ClickHouseOptions
	{
		private readonly IConfiguration _configuration;

		public ExcelOptions(IConfiguration configuration) : base(configuration)
		{
			_configuration = configuration;
		}

		public string[] Sheets => string.IsNullOrWhiteSpace(_configuration["Sheets"])
			? new string[0]
			: _configuration["Sheets"].Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet().ToArray();

		public int StartRow => string.IsNullOrWhiteSpace(_configuration["StartRow"])
			? 1
			: int.Parse(_configuration["StartRow"]);

		public int StartSheet => string.IsNullOrWhiteSpace(_configuration["StartSheet"])
			? -1
			: int.Parse(_configuration["StartSheet"]);
	}
}