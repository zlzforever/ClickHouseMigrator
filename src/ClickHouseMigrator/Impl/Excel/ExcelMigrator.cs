using System;
using System.Collections.Generic;
using System.Linq;
using ClosedXML.Excel;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator.Impl.Excel
{
	public class ExcelMigrator : Migrator
	{
		private ExcelOptions _options;
		private XLWorkbook _workbook;
		private Dictionary<string, string> _columnLetters;
		private Dictionary<string, ColumnDefine> _columns;
		private IXLWorksheet _currentWorksheet;
		private int _currentWorksheetIndex;
		private List<string> _sheets;
		private int _currentRow;

		protected override Dictionary<string, ColumnDefine> FetchTablesColumns()
		{
			if (_columns != null)
			{
				return _columns;
			}

			var columnCount = _currentWorksheet.ColumnCount();
			var lastColumnLetter = _currentWorksheet.LastColumnUsed().Cell(1).Address.ColumnLetter;
			_columns = new Dictionary<string, ColumnDefine>();
			_columnLetters = new Dictionary<string, string>();
			for (var i = 1; i <= columnCount; ++i)
			{
				var cell = _currentWorksheet.Cell(_options.StartRow, i);
				var value = cell?.Value?.ToString();
				if (value == null)
				{
					break;
				}

				value = value.Trim().ToLower();
				if (string.IsNullOrWhiteSpace(value))
				{
					break;
				}

				_columns.Add(value, new ColumnDefine
				{
					Name = value,
					DataType = value.Contains("时间") ? "DateTime" : "String",
					IsPrimary = false
				});
				_columnLetters.Add(value, cell.Address.ColumnLetter);

				if (cell.Address.ColumnLetter == lastColumnLetter)
				{
					break;
				}
			}

			_currentRow = _options.StartRow;
			return _columns;
		}

		protected override List<object[]> FetchRows(int count)
		{
			var list = new List<object[]>();
			for (int i = 0; i < count; ++i)
			{
				try
				{
					var data = GetRowData(_currentWorksheet, _currentRow);

					if (data == null)
					{
						if (NextWorksheet())
						{
							// 跳过标题
							_currentRow = _options.StartRow;
							continue;
						}
						else
						{
							break;
						}
					}

					list.Add(data.ToArray());

					_currentRow++;
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
				}
			}

			return list;
		}

		private List<object> GetRowData(IXLWorksheet worksheet, int row)
		{
			var data = new List<object>();
			var empty = true;
			foreach (var column in _columns)
			{
				var letter = _columnLetters[column.Key];
				var named = $"{letter}{row + 1}";
				var cellValue = worksheet.Cell(named).Value;
				var value = cellValue == null ? "" : cellValue.ToString();
				if (!string.IsNullOrWhiteSpace(value))
				{
					empty = false;
				}

				if (column.Value.DataType == "String")
				{
					data.Add(value);
				}
				else
				{
					if (value == "0000-00-00 00:00:00" || string.IsNullOrWhiteSpace(value))
					{
						data.Add(DateTime.MinValue);
					}
					else
					{
						data.Add(DateTime.TryParse(value, out var time) ? time : DateTime.MinValue);
					}
				}
			}

			if (empty)
			{
				return null;
			}

			return data;
		}

		private bool NextWorksheet()
		{
			// 未配置 Sheets 则只导入第一个 Sheet
			if (_sheets == null || _sheets.Count == 0)
			{
				return false;
			}
			else
			{
				var index = _currentWorksheetIndex + 1;
				if (index >= _sheets.Count)
				{
					return false;
				}

				var next = _sheets[index];
				_currentWorksheetIndex++;
				_currentWorksheet = _workbook.Worksheet(next);
				return true;
			}
		}

		protected override Dictionary<string, string> GetSwitchMappings()
		{
			return new Dictionary<string, string>
			{
				{"--file", "File"},

				{"--sheets", "Sheets"},

				{"--startRow", "StartRow"},
				{"--sheetStart", "SheetStart"}
			};
		}

		protected override void Initialize(IConfiguration configuration)
		{
			_options = new ExcelOptions(configuration);
			_workbook = new XLWorkbook(_options.File);

			if (_options.Sheets.Length > 0)
			{
				_sheets = _options.Sheets.ToList();

				foreach (var sheet in _sheets)
				{
					if (_workbook.Worksheets.All(x => x.Name != sheet))
					{
						throw new Exception($"Sheet {sheet} 不存在");
					}
				}

				_currentWorksheet = _workbook.Worksheets.Worksheet(_sheets[0]);
			}

			if (_currentWorksheet == null && _options.SheetStart >= 0)
			{
				var names = _workbook.Worksheets.Select(x => x.Name).ToList();
				_sheets = names.Skip(_options.SheetStart).ToList();
				_currentWorksheet = _workbook.Worksheets.Worksheet(_sheets[0]);
			}

			// 未配置任何 sheet 和 Start 则只取第一个 Sheet
			_currentWorksheet ??= _workbook.Worksheets.First();

			_currentWorksheetIndex = 0;
		}
	}
}