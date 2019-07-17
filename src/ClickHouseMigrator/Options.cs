using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator
{
	public class Options
	{
		private readonly IConfiguration _configuration;

		public Options(IConfiguration configuration)
		{
			_configuration = configuration;
		}

		/// <summary>
		/// Source database
		/// </summary>
		public string DataSource => string.IsNullOrWhiteSpace(_configuration["DataSource"])
			? "mysql"
			: _configuration["DataSource"].Trim();

		/// <summary>
		/// Source database host
		/// </summary>
		public string SourceHost => string.IsNullOrWhiteSpace(_configuration["SourceHost"])
			? "localhost"
			: _configuration["SourceHost"].Trim();

		/// <summary>
		/// Source database port
		/// </summary>
		public int SourcePort => string.IsNullOrWhiteSpace(_configuration["SourcePort"])
			? 0
			: int.Parse(_configuration["SourcePort"].Trim());

		/// <summary>
		/// Source database user
		/// </summary>
		public string SourceUser => string.IsNullOrWhiteSpace(_configuration["SourceUser"])
			? null
			: _configuration["SourceUser"].Trim();

		/// <summary>
		/// Source database password
		/// </summary>
		public string SourcePassword => string.IsNullOrWhiteSpace(_configuration["SourcePassword"])
			? null
			: _configuration["SourcePassword"].Trim();

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

		/// <summary>
		/// Source database to migrate
		/// </summary>
		public string SourceDatabase => string.IsNullOrWhiteSpace(_configuration["SourceDatabase"])
			? null
			: _configuration["SourceDatabase"].Trim();

		/// <summary>
		/// Source table to migrate
		/// </summary>
		public string SourceTable => string.IsNullOrWhiteSpace(_configuration["SourceTable"])
			? null
			: _configuration["SourceTable"].Trim();

		/// <summary>
		/// Target database migrate to
		/// </summary>
		public string TargetDatabase => string.IsNullOrWhiteSpace(_configuration["TargetDatabase"])
			? SourceDatabase
			: _configuration["TargetDatabase"].Trim();

		/// <summary>
		/// Target table migrate to
		/// </summary>
		public string TargetTable => string.IsNullOrWhiteSpace(_configuration["TargetTable"])
			? SourceTable
			: _configuration["TargetTable"].Trim();

		/// <summary>
		/// If drop target table if exists
		/// </summary>
		public bool Drop => !string.IsNullOrWhiteSpace(_configuration["Drop"]) &&
		                    bool.Parse(_configuration["Drop"].Trim());

		/// <summary>
		/// Ignore database, table name, column name case
		/// </summary>
		public bool Lowercase => string.IsNullOrWhiteSpace(_configuration["Lowercase"]) ||
		                         bool.Parse(_configuration["Lowercase"].Trim());

		/// <summary>
		/// Submit count of a batch
		/// </summary>
		public int Batch
		{
			get
			{
				var batch = string.IsNullOrWhiteSpace(_configuration["Batch"])
					? 5000
					: int.Parse(_configuration["Batch"].Trim());

				if (batch < 1000)
				{
					batch = 1000;
				}

				//preventing SQL Exception about "The server supports a maximum of 2000 parameters"
				if (DataSource == "mssql" && batch > 2000)
				{
					batch = 2000;
				}

				return batch;
			}
		}

		/// <summary>
		/// Thread count
		/// </summary>
		public int Thread => string.IsNullOrWhiteSpace(_configuration["Thread"])
			? Environment.ProcessorCount
			: int.Parse(_configuration["Thread"].Trim());

		/// <summary>
		/// Trace performance
		/// </summary>
		public bool Trace => !string.IsNullOrWhiteSpace(_configuration["Trace"]) &&
		                     bool.Parse(_configuration["Trace"].Trim());

		/// <summary>
		/// Migrate mode: parallel or sequential, when use sequential thread and batch are useless
		/// </summary>
		public string Mode => string.IsNullOrWhiteSpace(_configuration["Mode"])
			? "parallel"
			: _configuration["Mode"].Trim();

		/// <summary>
		/// Write file log
		/// </summary>
		public bool Log => !string.IsNullOrWhiteSpace(_configuration["Log"]) &&
		                   bool.Parse(_configuration["Log"].Trim());

		/// <summary>
		/// Order by for clickhouse
		/// </summary>
		public string[] OrderBy => string.IsNullOrWhiteSpace(_configuration["OrderBy"])
			? new string[0]
			: _configuration["OrderBy"].Trim().Split(',');


		public string GetTargetDatabase()
		{
			return Lowercase ? TargetDatabase.ToLowerInvariant() : TargetDatabase;
		}

		public string GetTargetTable()
		{
			return Lowercase ? TargetTable.ToLowerInvariant() : TargetTable;
		}
	}
}