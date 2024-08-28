from simple_salesforce import Salesforce
import pandas as pd
import configparser
import sys, os

# env_nm=str(os.sys.argv[1])
# soql_query=str(os.sys.argv[2])
def sf_query_execution(env_nm,soql_query):
    config_loc='./configuration.confg'
    config=configparser.RawConfigParser()
    config.read(config_loc)
    if env_nm.lower()=='uat':
        items=dict(config.items('uat-salesforce'))
    elif env_nm.lower()=='qa':
        items=dict(config.items('qa-salesforce'))
    elif env_nm.lower()=='prod':
        items=dict(config.items('prod-salesforce'))
    else:
        print('Please enter correct environment ...')

    usrnm=items.get('username')
    pswd=items.get('password')
    token=items.get('security_token')
    url=items.get('url')

    try:
        # print('Connecting to Salesforce ...')
        sf=Salesforce(username=usrnm, password=pswd, security_token=token, domain='test', 
                    instance_url=url)
        # print('Connection Successful !')
    except Exception as e:
        print(f'Unale to establish salesforce connection due to {e}')

    
    resp=sf.query_all(soql_query)
    return_rec=resp['records']
    for res in return_rec:
        res.pop('attributes',None)

    df=pd.DataFrame(return_rec)
    # df.to_csv("C:\\Users\\SAyubi\\Documents\\list.csv",index=False)
    # print(f'Total Return records count is {len(df)}')
    if len(df)==1:
        ret_val=df[soql_query.split(' ')[1]][0]
    else:
        ret_val=0
        print('length of Dataframe is 0 or more than 1 ...')
        print(f'sql query is : {soql_query}')
    return ret_val

# soql_query="select Id,Name from operatinghours where Club_Code__c='195'"
# sf_query_execution('uat',soql_query)
