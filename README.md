This Snowflake Stored Procedure uses Python code to read a Excel file from a external stage and can be used as part of a automated data pipeline.

To call the stored procedure run the commande below using the following parameters: <br>
-Path to Excel file: @stage/filename.xlsx <br>
-Excel Sheet Name: Sheet1 <br>
-Table full path DATABASE.SCHEMA.TABLE <br>
-Mode: Append or Overwrite <br>
<br>
CALL excel_to_snowflake('@stage/test1.xlsx', 'sheet2', 'database.public.demo_test_table', 'Overwrite');
