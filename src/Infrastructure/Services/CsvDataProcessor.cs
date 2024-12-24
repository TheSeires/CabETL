using System.Data;
using System.Globalization;
using System.Text;
using Core.Configuration;
using Core.Interfaces;

namespace Infrastructure.Services;

public class CsvDataProcessor : IDataProcessor
{
    private readonly IDataRepository _dataRepository;
    private readonly DataProcessorConfiguration _configuration;
    private readonly int _maxColumnIndex;
    private readonly Func<string[], string> _generateDuplicateKeyFunc;
    private readonly string _duplicatesFilePath;

    public CsvDataProcessor(IDataRepository dataRepository, Action<DataProcessorConfiguration> configure,
        Func<string[], string> generateDuplicateKeyFunc, string duplicatesFilePath)
    {
        _dataRepository = dataRepository;
        _configuration = new DataProcessorConfiguration();
        _generateDuplicateKeyFunc = generateDuplicateKeyFunc;
        _duplicatesFilePath = duplicatesFilePath;
        configure(_configuration);

        _maxColumnIndex = _configuration.ColumnMappings.Max(m => m.SourceColumnIndex);
    }

    public async Task ProcessAsync(string filePath)
    {
        var duplicates = new HashSet<string>();
        var dataTableBatch = new DataTable();
        InitializeColumns(dataTableBatch);

        using var reader = new StreamReader(filePath);
        var headerLine = await reader.ReadLineAsync();

        if (string.IsNullOrWhiteSpace(headerLine))
        {
            Console.WriteLine("Data file is empty");
            return;
        }

        string[] headers = headerLine.Split(',');
        ConfigureHeaders(headers);

        if (headers.Length < _configuration.ColumnMappings.Count)
        {
            throw new FormatException("Invalid CSV format: missing headers.");
        }

        var duplicatesCsvBuilder = new StringBuilder(headerLine + Environment.NewLine);

        var count = 0;
        var duplicatesCount = 0;
        var invalidCount = 0;
        while (reader.EndOfStream == false)
        {
            string? line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            string[]? values = line.Split(',');
            if (values.Length == 0 || values.Length < _maxColumnIndex)
            {
                continue;
            }

            string duplicateKey = GenerateDuplicateKey(values);
            var isDuplicate = duplicates.Add(duplicateKey) == false;

            if (isDuplicate)
            {
                duplicatesCount++;
                duplicatesCsvBuilder.AppendLine(line);
                continue;
            }

            var row = CreateRow(dataTableBatch, values);
            if (row == null)
            {
                invalidCount++;
                continue;
            }

            dataTableBatch.Rows.Add(row);

            if (dataTableBatch.Rows.Count >= _configuration.BatchSize)
            {
                await _dataRepository.BulkCopyAsync(dataTableBatch);
                dataTableBatch.Rows.Clear();
                Console.WriteLine($"Processed data batch ({count + 1})...");
            }

            count++;
        }

        if (dataTableBatch.Rows.Count > 0)
        {
            await _dataRepository.BulkCopyAsync(dataTableBatch);
        }

        Console.WriteLine("Successfully processed all the data");

        WriteDuplicatesToFile(duplicatesCsvBuilder);
        Console.WriteLine($"Invalid rows: {invalidCount}");
        Console.WriteLine($"Duplicate rows: {duplicatesCount}");
    }

    private string GenerateDuplicateKey(string[] values)
    {
        return _generateDuplicateKeyFunc.Invoke(values);
    }

    private void ConfigureHeaders(string[] headers)
    {
        var mappingsWithSourceColumnName = _configuration.ColumnMappings
            .Where(m => string.IsNullOrWhiteSpace(m.SourceColumnName) == false);

        foreach (var mapping in mappingsWithSourceColumnName)
        {
            var index = Array.IndexOf(headers, mapping.SourceColumnName);
            if (index == -1)
            {
                throw new ArgumentException("Invalid source column name.", nameof(mapping.SourceColumnName));
            }

            mapping.SourceColumnIndex = index;
        }
    }

    private DataRow? CreateRow(DataTable dataTable, string[] values)
    {
        var row = dataTable.NewRow();
        foreach (var mapping in _configuration.ColumnMappings)
        {
            if (mapping.SourceColumnIndex == -1)
            {
                throw new ArgumentNullException(nameof(mapping.SourceColumnIndex));
            }

            var value = mapping.Trim
                ? values[mapping.SourceColumnIndex].Trim()
                : values[mapping.SourceColumnIndex];

            var isValueNullOrWhiteSpace = string.IsNullOrWhiteSpace(value);
            if (mapping.RemoveRowIfNullOrEmpty && isValueNullOrWhiteSpace)
            {
                return null;
            }

            if (mapping.CustomConversion != null)
            {
                row[mapping.DbColumnName] = mapping.CustomConversion(value);
                continue;
            }

            if (isValueNullOrWhiteSpace)
            {
                row[mapping.DbColumnName] = DBNull.Value;
                continue;
            }

            if (mapping.Type == typeof(DateTime))
            {
                row[mapping.DbColumnName] = DateTime.Parse(value).ToUniversalTime();
            }
            else if (mapping.Type == typeof(decimal))
            {
                row[mapping.DbColumnName] = decimal.Parse(value, CultureInfo.InvariantCulture);
            }
            else if (mapping.Type.IsGenericType && mapping.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                row[mapping.DbColumnName] = Convert.ChangeType(value, Nullable.GetUnderlyingType(mapping.Type)!);
            }
            else
            {
                row[mapping.DbColumnName] = Convert.ChangeType(value, mapping.Type);
            }
        }

        return row;
    }

    private void InitializeColumns(DataTable dataTable)
    {
        foreach (var colMapping in _configuration.ColumnMappings)
        {
            dataTable.Columns.Add(colMapping.DbColumnName, colMapping.Type);
        }
    }

    private void WriteDuplicatesToFile(StringBuilder duplicatesCsvBuilder)
    {
        File.WriteAllText(_duplicatesFilePath, duplicatesCsvBuilder.ToString());
    }

    public Task<int> GetTotalRecordsProcessedAsync()
    {
        return _dataRepository.GetTotalRecordsAsync();
    }
}