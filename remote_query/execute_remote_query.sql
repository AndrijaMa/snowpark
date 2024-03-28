CREATE OR REPLACE FUNCTION exec_remote_query(account_name string, sql_command string, wh_name string, role_name string, user_name string)
RETURNS variant
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'get_data'
EXTERNAL_ACCESS_INTEGRATIONS = (snow_access_int)
PACKAGES = ('snowflake-snowpark-python','requests','pandas')
COMMENT = 'Working solution OAuth, passing on username as APP_USR'
SECRETS = ('cred' = snow_remote_token)
AS
$$
import _snowflake
import requests
import json
import pandas as pd
token = _snowflake.get_oauth_access_token('cred')

session = requests.Session()
# this can be parameterized
timeout = 60

def get_data(account_name, sql_command, wh_name, role_name, user_name):
    
    apiurl='https://'+account_name+'.snowflakecomputing.com/api/v2/statements/'
    statements = "set APP_USER='" + user_name + "';" + sql_command;
    jsonBody =  {
            'statement': statements,
            'timeout': timeout,
            'warehouse': wh_name.upper(),
            'role': role_name.upper(),
            "parameters": {
                "MULTI_STATEMENT_COUNT": 2
                }
            }
    header = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Snowflake-Account": account_name,
            "X-Snowflake-Authorization-Token-Type": "OAUTH"
        }
        
    response = session.post(apiurl, json=jsonBody, headers=header)
        
    responseJson = response.json()
    requestId = responseJson['requestId']
    statementHandle = responseJson["statementHandles"][1]
    statementResponseUrl = apiurl + statementHandle +'?requestId=' + requestId
    statementResponse = session.get(statementResponseUrl, json = jsonBody, headers = header)
    
    page=0
    pageCount = len(statementResponse.json()['resultSetMetaData']['partitionInfo'])
    data = []
    while page <= int(pageCount)-1:
        statementResponseUrl = apiurl + statementHandle +'?requestId=' + requestId
        finalStatementResponseUrl = statementResponseUrl + '&partition=' + str(page)
        finalResponse = session.get(finalStatementResponseUrl, json = jsonBody, headers = header)
        data.extend(finalResponse.json()['data'])
        page+=1
    
    return data
$$;
