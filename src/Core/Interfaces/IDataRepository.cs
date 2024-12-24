using System.Data;

namespace Core.Interfaces;

public interface IDataRepository
{
    Task BulkCopyAsync(DataTable dataTable);
    Task<int> GetTotalRecordsAsync();
}