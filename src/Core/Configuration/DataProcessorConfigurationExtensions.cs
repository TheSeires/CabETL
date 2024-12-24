namespace Core.Configuration;

public static class DataProcessorConfigurationExtensions
{
    public static DataProcessorConfiguration SetBatchSize(this DataProcessorConfiguration configuration, int batchSize)
    {
        configuration.BatchSize = batchSize;
        return configuration;
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        string sourceColumnName)
    {
        return AddColumnMapping<T>(configuration, sourceColumnName, null, sourceColumnName, null);
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        string sourceColumnName, Func<string, object> customConversion)
    {
        return AddColumnMapping<T>(configuration, sourceColumnName, null, sourceColumnName, customConversion);
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        string sourceColumnName, string dbColumnName)
    {
        return AddColumnMapping<T>(configuration, sourceColumnName, null, dbColumnName, null);
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        string sourceColumnName, string dbColumnName, Func<string, object> customConversion)
    {
        return AddColumnMapping<T>(configuration, sourceColumnName, null, dbColumnName, customConversion);
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        int sourceColumnIndex, string dbColumnName)
    {
        return AddColumnMapping<T>(configuration, null, sourceColumnIndex, dbColumnName, null);
    }

    public static DataProcessorConfiguration AddColumnMapping<T>(this DataProcessorConfiguration configuration,
        int sourceColumnIndex, string dbColumnName, Func<string, object> customConversion)
    {
        return AddColumnMapping<T>(configuration, null, sourceColumnIndex, dbColumnName, customConversion);
    }

    private static DataProcessorConfiguration AddColumnMapping<T>(DataProcessorConfiguration configuration,
        string? sourceColumnName, int? sourceColumnIndex, string dbColumnName, Func<string, object?>? customConversion)
    {
        var columnMapping = new ColumnMapping
        {
            SourceColumnName = sourceColumnName,
            DbColumnName = dbColumnName,
            Type = typeof(T),
            CustomConversion = customConversion,
        };

        if (sourceColumnIndex.HasValue)
        {
            columnMapping.SourceColumnIndex = sourceColumnIndex.Value;
        }

        columnMapping.Validate();
        configuration.ColumnMappings.Add(columnMapping);
        return configuration;
    }
}