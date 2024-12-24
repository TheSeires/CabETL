namespace Core.Interfaces;

public interface IDataProcessor
{
    Task ProcessAsync(string filePath);
    Task<int> GetTotalRecordsProcessedAsync();
}