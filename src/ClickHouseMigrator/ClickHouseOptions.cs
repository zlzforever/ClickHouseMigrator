using System;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator
{
	public class ClickHouseOptions
	{
		private readonly IConfiguration _configuration;

		public ClickHouseOptions(IConfiguration configuration)
		{
			_configuration = configuration;
		}

		/// <summary>
		/// Clickhouse host
		/// </summary>
		public string Host => string.IsNullOrWhiteSpace(_configuration["Host"])
			? "localhost"
			: _configuration["Host"].Trim();

		/// <summary>
		/// Clickhouse port
		/// </summary>
		public int Port => string.IsNullOrWhiteSpace(_configuration["Port"])
			? 9000
			: int.Parse(_configuration["Port"].Trim());

		/// <summary>
		/// Clickhouse user
		/// </summary>
		public string User => string.IsNullOrWhiteSpace(_configuration["User"])
			? null
			: _configuration["User"].Trim();

		/// <summary>
		/// Clickhouse password
		/// </summary>
		public string Password => string.IsNullOrWhiteSpace(_configuration["Password"])
			? null
			: _configuration["Password"].Trim();

		public string[] OrderBy => string.IsNullOrWhiteSpace(_configuration["OrderBy"])
			? new string[0]
			: _configuration["OrderBy"].Split(',', StringSplitOptions.RemoveEmptyEntries);

		public string Database => _configuration["Database"];

		public string Table => _configuration["Table"];

		public int BatchSize => string.IsNullOrWhiteSpace(_configuration["BatchSize"])
			? 1000
			: int.Parse(_configuration["BatchSize"]);

		public bool DropTable => !string.IsNullOrWhiteSpace(_configuration["DropTable"]) && bool.Parse(_configuration["DropTable"]);
	}
}