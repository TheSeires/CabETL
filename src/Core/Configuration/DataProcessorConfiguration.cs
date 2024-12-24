namespace Core.Configuration;

public class DataProcessorConfiguration
{
    public int BatchSize { get; internal set; } = 10_000;
    public List<ColumnMapping> ColumnMappings { get; } = [];
}