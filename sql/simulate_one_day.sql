USE WideWorldImporters;
GO

/* ------------------------------------------------------------
   0. Safety net: run ONCE before the first test, then leave
      commented out. Restore from this if a run goes wrong.
   ------------------------------------------------------------ */
 --BACKUP DATABASE WideWorldImporters
 --TO DISK = N'/var/opt/mssql/backup/wwi_before_simulation.bak'
 --WITH INIT, COMPRESSION, STATS = 10;
 --GO

/* ------------------------------------------------------------
   1. Advance the source by exactly one business day
   ------------------------------------------------------------ */
SET NOCOUNT ON;

DECLARE @NextDate DATE =
    DATEADD(DAY, 1, (SELECT MAX(OrderDate) FROM Sales.Orders));

PRINT CONCAT('Simulating ', CONVERT(VARCHAR(10), @NextDate, 23),
             ' (', DATENAME(WEEKDAY, @NextDate), ')');

BEGIN TRY
    -- Create the ~19 helper procedures (they don't exist between runs)
    EXEC DataLoadSimulation.Configuration_ApplyDataLoadSimulationProcedures;

    -- Same date for start and end = one pass through the WHILE loop
    EXEC DataLoadSimulation.DailyProcessToCreateHistory
        @StartDate = @NextDate,
        @EndDate   = @NextDate,
        @AverageNumberOfCustomerOrdersPerDay = 60,
        @SaturdayPercentageOfNormalWorkDay   = 50,
        @SundayPercentageOfNormalWorkDay     = 30,
        @UpdateCustomFields = 0,
        @IsSilentMode       = 1,
        @AreDatesPrinted    = 1;

    -- Drop the helpers again
    EXEC DataLoadSimulation.Configuration_RemoveDataLoadSimulationProcedures;
END TRY
BEGIN CATCH
    -- Always tear down, even on failure, then re-raise the error
    EXEC DataLoadSimulation.Configuration_RemoveDataLoadSimulationProcedures;
    THROW;
END CATCH;