## Usage
To run the program, follow these steps:
- Set up a database using [SQL scripts](https://github.com/TheSeires/CabETL/tree/main/src/Infrastructure/Migrations)
- Define the connection string for your database inside of [appsettings.json](https://github.com/TheSeires/CabETL/tree/main/src/Application/appsettings.json)
- Open a terminal and navigate to the Application folder
- Run command `dotnet run -c Release "path_to_the_csv_file.csv"` with the required argument for the CSV file path

Example: `dotnet run -c Release "C:\Users\pc\Downloads\sample-cab-data.csv"`

***Ensure you are inside the Application folder before running the command!***

## Handling Larger Data Files
If the program is expected to handle larger data files, such as a 10GB CSV, the following changes could be implemented to improve performance and scalability:

1. Parallel processing of CSV chunks, so the file would be processed in chunks using parallel processing.
   
2. Table partitioning by date or another logical key, to reduce query latency and optimize data insertion.

3. Further database optimization, such as more granular indexes, partitioned indexes, and compression settings could be applied to manage the larger dataset efficiently.

---

## SQL Scripts
The SQL scripts used for creating the database and table can be found here: [link](https://github.com/TheSeires/CabETL/tree/main/src/Infrastructure/Migrations)

---

## Results
- Number of rows after running the program: `29,840`
- Duplicate rows: `111`
- Invalid rows: `49`

---

## Implemented
1. Indexes for Queries:
   - Created indexes for commonly used query scenarios to improve database performance.
   
2. Computed Column:
   - Added a computed column to calculate trip duration dynamically, optimizing query results.

3. Batch Processing:
   - Used batch processing both for bulk data insertion to improve performance and avoid memory overflows.

4. Whitespace Trimming:
   - Trimmed unnecessary whitespace for all text-based columns.

5. Date Conversion:
   - Converted all dates from EST to UTC before inserting them into the database as required.

6. Removed duplicates logging:
   - Writes all removed duplicates into a `duplicates.csv` file inside of `Application` folder.
