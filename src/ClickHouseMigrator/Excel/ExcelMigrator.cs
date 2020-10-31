using System;
using System.Collections.Generic;
using System.Linq;
using ClosedXML.Excel;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator.Excel
{
	public class ExcelMigrator : Migrator
	{
		private ExcelOptions _excelOptions;
		private List<dynamic[]> _data;
		private List<ColumnDefine> _columns;
		private int _row;

		protected override List<ColumnDefine> FetchTablesColumns()
		{
			return _columns;
		}

		protected override (dynamic[][], int) FetchRows(int count)
		{
			var list = new dynamic[count][];
			var i = 0;
			for (; i < count; ++i)
			{
				if (_row >= _data.Count)
				{
					break;
				}

				var data = _data[_row];

				if (data == null)
				{
					break;
				}

				list[i] = data;

				_row++;
			}

			return (list, i);
		}

		protected override Dictionary<string, string> GetSwitchMappings()
		{
			return new Dictionary<string, string>
			{
				{"--sheets", "Sheets"},
				{"--start-row", "StartRow"},
				{"--sheet-start", "StartSheet"}
			};
		}

		protected override void Initialize(IConfiguration configuration)
		{
			_excelOptions = new ExcelOptions(configuration);
			var workbook = new XLWorkbook(_excelOptions.File);

			if (workbook.Worksheets.Count == 0)
			{
				throw new ArgumentException("There is no available sheet in the workbook");
			}

			List<string> sheets = null;

			if (_excelOptions.Sheets.Length > 0)
			{
				sheets = _excelOptions.Sheets.ToList();

				foreach (var sheet in sheets)
				{
					if (workbook.Worksheets.All(x => x.Name != sheet))
					{
						throw new ArgumentException($"Sheet {sheet} is not exist");
					}
				}
			}

			if (sheets == null && _excelOptions.StartSheet >= 0)
			{
				sheets = workbook.Worksheets.Select(x => x.Name).Skip(_excelOptions.StartSheet - 1).ToList();
			}

			sheets ??= new List<string> {workbook.Worksheets.First().Name};

			_columns = GetColumnDefines(workbook.Worksheet(sheets[0]));
			_data = new List<dynamic[]>();

			foreach (var sheet in sheets)
			{
				var worksheet = workbook.Worksheet(sheet);

				foreach (var row in worksheet.Rows())
				{
					if (row.RowNumber() <= _excelOptions.StartRow)
					{
						continue;
					}

					var data = GetRowData(row, _columns);
					if (data != null)
					{
						_data.Add(data);
					}
					else
					{
						break;
					}
				}
			}
		}

		private dynamic[] GetRowData(IXLRow row, List<ColumnDefine> columns)
		{
			var data = new dynamic[columns.Count];
			var isEmpty = true;
			foreach (var column in columns)
			{
				var value = row.Cell(column.Index)?.Value?.ToString()?.Trim();

				if (!string.IsNullOrWhiteSpace(value))
				{
					isEmpty = false;
				}

				object result = value;

				if (column.DataType == "DateTime")
				{
					if (value == "0000-00-00 00:00:00" || string.IsNullOrWhiteSpace(value))
					{
						result = DateTime.MinValue;
					}
					else
					{
						result = DateTime.TryParse(value, out var time) ? time : DateTime.MinValue;
					}
				}

				data[column.Index - 1] = result;
			}

			return isEmpty ? null : data;
		}


		private List<ColumnDefine> GetColumnDefines(IXLWorksheet worksheet)
		{
			var lastColumnLetter = worksheet.LastColumnUsed().Cell(1).Address.ColumnLetter;
			var columns = new List<ColumnDefine>();

			foreach (var header in worksheet.Row(1).Cells())
			{
				var value = header?.Value?.ToString()?.Trim();
				// 标题内容为空认为表格到底
				if (string.IsNullOrWhiteSpace(value))
				{
					break;
				}

				columns.Add(new ColumnDefine
				{
					Index = header.Address.ColumnNumber,
					Name = value,
					DataType = value.Contains("时间") || value.Contains("日期") ? "DateTime" : "String",
					IsPrimary = false
				});

				if (header.Address.ColumnLetter == lastColumnLetter)
				{
					break;
				}
			}

			return columns;
		}
	}
}