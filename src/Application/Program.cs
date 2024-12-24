using System.Globalization;
using Core.Configuration;
using Infrastructure.Data;
using Infrastructure.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

try
{
    if (args.Length == 0)
    {
        throw new ArgumentException("Please provide the CSV file path as an argument.");
    }

    string csvFilePath = args[0];
    ValidateCsvFilePath(csvFilePath);

    var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

#if DEBUG
    builder.AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true);
#endif

    IConfigurationRoot configuration = builder.Build();
    var connectionString = configuration.GetConnectionString("DefaultConnection");

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        throw new ArgumentException(
            "Please provide the database connection string inside of configuration file 'appsettings.json'.");
    }

    var repository = new CabTripRepository(connectionString, 5_000, ConfigureBulkCopyColMappings);
    await repository.TruncateTableAsync();

    var csvDataProcessor =
        new CsvDataProcessor(repository, ConfigureCsvDataProcessor, GenerateDuplicateKeyFunc, "duplicates.csv");

    await csvDataProcessor.ProcessAsync(csvFilePath);
    int rowsInDatabaseTable = await repository.GetTotalRecordsAsync();
    Console.WriteLine($"Total rows in table: {rowsInDatabaseTable}");

    Console.WriteLine("Press any key to exit...");
    Console.ReadKey();
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
#if DEBUG
    Console.WriteLine(ex);
#else
    Console.WriteLine($"Error: {ex.Message}");
#endif
    Console.ResetColor();

    Console.WriteLine("Press any key to exit...");
    Console.ReadKey();
}

void ValidateCsvFilePath(string csvFilePath)
{
    if (File.Exists(csvFilePath) == false)
    {
        throw new FileNotFoundException("The specified CSV file does not exist.", csvFilePath);
    }

    if (Path.GetExtension(csvFilePath) != ".csv")
    {
        throw new ArgumentException("The specified file is not a .csv file.");
    }
}

void ConfigureBulkCopyColMappings(SqlBulkCopyColumnMappingCollection mappingCollection)
{
    mappingCollection.Add("tpep_pickup_datetime", "PickupDateTime");
    mappingCollection.Add("tpep_dropoff_datetime", "DropoffDateTime");
    mappingCollection.Add("passenger_count", "PassengerCount");
    mappingCollection.Add("trip_distance", "TripDistance");
    mappingCollection.Add("store_and_fwd_flag", "StoreAndFwdFlag");
    mappingCollection.Add("PULocationID", "PULocationID");
    mappingCollection.Add("DOLocationID", "DOLocationID");
    mappingCollection.Add("fare_amount", "FareAmount");
    mappingCollection.Add("tip_amount", "TipAmount");
}

void ConfigureCsvDataProcessor(DataProcessorConfiguration dataProcessorConfiguration)
{
    var dataTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    dataProcessorConfiguration.SetBatchSize(10_000)
        .AddColumnMapping<DateTime>("tpep_pickup_datetime", value => ToCustomDateTimeFormat(value, dataTimeZone))
        .AddColumnMapping<DateTime>("tpep_dropoff_datetime", value => ToCustomDateTimeFormat(value, dataTimeZone))
        .AddColumnMapping<int>("passenger_count")
        .AddColumnMapping<decimal>("trip_distance")
        .AddColumnMapping<string>("store_and_fwd_flag", value => value == "Y" ? "Yes" : "No")
        .AddColumnMapping<int>("PULocationID")
        .AddColumnMapping<int>("DOLocationID")
        .AddColumnMapping<decimal>("fare_amount")
        .AddColumnMapping<decimal>("tip_amount");
}

object ToCustomDateTimeFormat(string value, TimeZoneInfo tzi)
{
    if (DateTime.TryParseExact(value, "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var parsedDate) == false)
    {
        throw new FormatException(
            $"Invalid date format: '{value}'. Make sure your data uses format 'MM/dd/yyyy hh:mm:ss tt'.");
    }

    var localDateTime = new DateTimeOffset(parsedDate, tzi.GetUtcOffset(parsedDate));
    return localDateTime.UtcDateTime;
}

string GenerateDuplicateKeyFunc(string[] values)
{
    return $"{values[1]}_{values[2]}_{values[3]}";
}