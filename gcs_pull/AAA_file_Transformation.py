import pandas as pd
from salesforce_soqlExec import sf_query_execution
import re
import ast
from datetime import datetime

masterDf=pd.read_excel("C:\\Users\\sayubi\\Documents\\files_to_process\\observation.xlsx", sheet_name='Sheet2')
gpcDf=pd.read_excel("C:\\Users\\sayubi\\Documents\\files_to_process\\observation.xlsx", sheet_name='Global_param')
# print(masterDf.columns)

df1=pd.read_excel("C:\\Users\\sayubi\\Documents\\files_to_process\\195_FacilityAccount_raw_file.xlsx")
print('Eliminating trailing spaces across the sheet ...')
df1=df1.applymap(lambda x: x.strip() if isinstance(x,str) else x)
df2=pd.DataFrame(columns=masterDf.columns.str.replace('.1','',regex=False))
df2=df2.fillna('')
# print(df2.columns)

# print(masterDf['INTEGRATION_ID__C'][0])
for i in range(0,len(masterDf.columns)):
    col=masterDf.columns[i]
    slice_pos=int(gpcDf['Slice_pos'][0])
    if str(col).upper().startswith('XREF_') and 'rename' in str(masterDf[col][0]).lower():
        fetchCol=str(masterDf[col][0]).replace('rename','').strip()
        if str(col).upper().replace('XREF_','') in df1.columns:
            df2[col]=df1[fetchCol]
            # df2.rename(columns={col:str(masterDf[col][0]).replace('rename as','').strip()},inplace=True)
    elif col in df1.columns and 'same' in str(masterDf[col][0]).lower():
        df2[col]=df1[col]
    elif str(masterDf[col][0]).lower().startswith('concat'):
        concat_val=str(masterDf[col][0]).replace('concat','').strip().split(',')
        if len(concat_val)==2:
            if '$' in str(concat_val):
                for i in concat_val:
                    if '$' in i:
                        print('Global Variable found ...')
                        print('Checking the value of Global Parameter from gpc ...')
                        # df2.reset_index(inplace=True)
                        df2[col]=str(gpcDf[i.replace('$','')][0])+'-'+df1[concat_val[1]].astype(str)
                        # print(f'First column values is {df2[col]}')
            else:
                print('No Global parameter value to be fetched !')
                df2[col]=(df1[concat_val[0]].astype(str)+'-'+df1[concat_val[1]].astype(str)).str[:slice_pos]
        else:
            print('Values to be concatenated is exceeding the limit of 2 !')
    elif str(masterDf[col][0]).startswith('$'):
        col_val=str(masterDf[col][0]).replace('$','')
        df2[col]=gpcDf[col_val][0]
    elif str(masterDf[col][0]).lower().startswith('select '):
        col_val=str(masterDf[col][0])
        if '$' and '#' in col_val:
            gpc_col=re.search(r'\$[^\s]+',col_val).group(0)
            gpc_val=str(gpcDf[gpc_col.replace('$','')][0])
            ref_col=re.search(r'\#[^\s]+',col_val).group()
            soql_query=col_val.replace(gpc_col,repr(gpc_val)).replace(ref_col,repr('{x}'))
            print(soql_query)
            df2[col]=df1[ref_col.replace('#','')].apply(lambda row:sf_query_execution('qa',soql_query.format(x=row)))
        elif '$' in col_val:
            gpc_col=re.search(r'\$[^\s]+',col_val).group(0)
            gpc_val=str(gpcDf[gpc_col.replace('$','')][0])
            soql_query=col_val.replace(gpc_col,repr(gpc_val))
            df2[col]=sf_query_execution('qa',soql_query)
        else:
            df2[col]=sf_query_execution('qa',col_val)
    elif str(masterDf[col][0]).lower().startswith('convert'):
        beforeCol=masterDf.columns[i-1]
        col_val=str(masterDf[col][0])
        action_dict=ast.literal_eval(col_val.replace('convert','').strip())
        df2[col]=df2[beforeCol].apply(lambda val: action_dict[val.upper()] if val in action_dict else val.title())
    elif str(masterDf[col][0]).lower().startswith('date'):
        beforeCol=masterDf.columns[i-1]
        df2[col]=df2[beforeCol].apply(lambda val:datetime.strptime(str(val),'%d-%b-%y').strftime("%Y-%m-%d") if pd.notna(val) else val)

    




print(df2.to_excel("C:\\Users\\sayubi\\Documents\\files_to_process\\195_FacilityAccount_prep_file.xlsx",index=False))
