using System.Data;
using Core.Interfaces;
using Microsoft.Data.SqlClient;

namespace Infrastructure.Data;

public class CabTripRepository : IDataRepository
{
    private readonly string _connectionString;
    private readonly int _batchSize;
    private readonly Action<SqlBulkCopyColumnMappingCollection> _configureBulkCopyColMappings;

    public CabTripRepository(string connectionString, int batchSize,
        Action<SqlBulkCopyColumnMappingCollection> configureBulkCopyColMappings)
    {
        _connectionString = connectionString;
        _batchSize = batchSize;
        _configureBulkCopyColMappings = configureBulkCopyColMappings;
    }

    public async Task TruncateTableAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = new SqlCommand("TRUNCATE TABLE CabTripData", connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task BulkCopyAsync(DataTable dataTable)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var bulkCopy = new SqlBulkCopy(connection);
        bulkCopy.BatchSize = _batchSize;
        bulkCopy.DestinationTableName = "CabTripData";

        ConfigureBulkCopyMappings(bulkCopy);

        await bulkCopy.WriteToServerAsync(dataTable);
    }

    private void ConfigureBulkCopyMappings(SqlBulkCopy bulkCopy)
    {
        _configureBulkCopyColMappings.Invoke(bulkCopy.ColumnMappings);
    }

    public async Task<int> GetTotalRecordsAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = new SqlCommand("SELECT COUNT(*) FROM CabTripData", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
}