This Snowflake Stored Procedure uses Python code to read a Excel file from a external stage and can be used as part of a automated data pipeline.

To call the stored procedure run the commande below using the following parameters:
-Path to Excel file: @stage/filename.xlsx 
-Excel Sheet Name: Sheet1
-Table full path DATABASE.SCHEMA.TABLE
-Mode: Append or Overwrite

CALL excel_to_snowflake('@stage/test1.xlsx', 'sheet2', 'database.public.demo_test_table', 'Overwrite');
