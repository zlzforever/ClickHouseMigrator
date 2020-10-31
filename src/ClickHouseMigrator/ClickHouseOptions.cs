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
			? "default"
			: _configuration["User"].Trim();

		/// <summary>
		/// Clickhouse password
		/// </summary>
		public string Password => string.IsNullOrWhiteSpace(_configuration["Password"])
			? null
			: _configuration["Password"].Trim();

		public string Database => _configuration["Database"];

		public string Table => _configuration["Table"];
		
		public string File => _configuration["File"];

		public int Batch => string.IsNullOrWhiteSpace(_configuration["Batch"])
			? 10000
			: int.Parse(_configuration["Batch"]);

		public bool DropTable => !string.IsNullOrWhiteSpace(_configuration["DropTable"]) &&
		                         bool.Parse(_configuration["DropTable"]);

		public bool Lowercase => !string.IsNullOrWhiteSpace(_configuration["Lowercase"]) &&
		                         bool.Parse(_configuration["Lowercase"]);

		/// <summary>
		/// Clickhouse host
		/// </summary>
		public string SourceHost => string.IsNullOrWhiteSpace(_configuration["SourceHost"])
			? "localhost"
			: _configuration["SourceHost"].Trim();

		/// <summary>
		/// Clickhouse port
		/// </summary>
		public int SourcePort => string.IsNullOrWhiteSpace(_configuration["SourcePort"])
			? 3306
			: int.Parse(_configuration["SourcePort"].Trim());

		/// <summary>
		/// Clickhouse user
		/// </summary>
		public string SourceUser => string.IsNullOrWhiteSpace(_configuration["SourceUser"])
			? "root"
			: _configuration["SourceUser"].Trim();

		/// <summary>
		/// Clickhouse password
		/// </summary>
		public string SourcePassword => string.IsNullOrWhiteSpace(_configuration["SourcePassword"])
			? null
			: _configuration["SourcePassword"].Trim();

		public string SourceDatabase => string.IsNullOrWhiteSpace(_configuration["SourceDatabase"])
			? null
			: _configuration["SourceDatabase"].Trim();

		public string SourceTable => string.IsNullOrWhiteSpace(_configuration["SourceTable"])
			? null
			: _configuration["SourceTable"].Trim();

		public int Thread => string.IsNullOrWhiteSpace(_configuration["Thread"])
			? Environment.ProcessorCount
			: int.Parse(_configuration["Thread"].Trim());
	}
}