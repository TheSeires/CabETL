namespace Core.Configuration;

public class ColumnMapping
{
    public string? SourceColumnName { get; init; }
    public int SourceColumnIndex { get; set; } = -1;
    public required string DbColumnName { get; init; }
    public required Type Type { get; init; }
    public bool Trim { get; init; } = true;
    public Func<string, object?>? CustomConversion { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(SourceColumnName) && SourceColumnIndex == -1)
            throw new ArgumentException(
                $"You must provide either {nameof(SourceColumnName)} or {nameof(SourceColumnIndex)}.");

        if (string.IsNullOrWhiteSpace(DbColumnName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(DbColumnName));
        }
    }
}