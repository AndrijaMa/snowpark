CREATE OR REPLACE PROCEDURE excel_to_snowflake(file_path string, sheet string, target_table string, upload_mode string)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'main'
AS
$$
from openpyxl import load_workbook
import pandas as pd
def main(session, file_path, sheet, target_table, upload_mode):

    file = session.file.get_stream(file_path)
    df = pd.read_excel(file,sheet)
    
    session_df = session.create_dataframe(df)
    session_df.write.mode(upload_mode).save_as_table(target_table)
    session_df.close()
    return 'done'    
$$;


call excel_to_snowflake('@excel/test1.xlsx', 'Sheet2', 'database.public.demo_test_table', 'Overwrite');
