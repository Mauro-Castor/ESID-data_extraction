from re import A
import sys, requests, json, logging, os, pprint, random, time
import pandas as pd

#os.chdir ('C:\Users\MauricioAguirreMoral\Desktop\Mauricio\Visual_Studio')
logging.captureWarnings(True)

token=(open('token').read())
token=(token.strip())
domain = "data"
study_id='B1D40E35-55A9-A967-F9D7-48FE0CA6F2A9'

api_call_headers = {'Authorization': 'Bearer ' + str(token)}

def get_new_token():

    api_call_headers = {'Authorization': 'Bearer ' + token}
    x=requests.get("https://"+domain+".castoredc.com/api/study/"+study_id+"/field",headers=api_call_headers)
    print(x)

    if str(x) !='<Response [200]>':

        auth_server_url = "https://"+domain+".castoredc.com/oauth/token"
        client_id = '79EBCE1C-2622-4F78-8414-2828581055D8'
        client_secret = '0079adaa3a1b1dc19a104ae799578caf'

        token_req_payload = {'grant_type': 'client_credentials'}

        token_response = requests.post(auth_server_url,data=token_req_payload, auth=(client_id, client_secret))
             
        if token_response.status_code !=200:
            print("Ups something went wrong with generating the new token", file=sys.stderr)
        tokens = json.loads(token_response.text)
        print('New token '+tokens['access_token'])
        open('token', 'w').write(tokens['access_token'])
        time.sleep(3)
        return {'Authorization': 'Bearer ' + str(tokens['access_token'])}
    else:
        time.sleep(3)
        return {'Authorization': 'Bearer ' + str(token)}
    
def field_results(api_call_headers):
    URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/data-points/study?page_size=1000")#
    x=requests.get(URL,headers=api_call_headers)
    # print(x)
    x=json.loads(x.text)
    pages=(x['page_count'])
    result = pd.DataFrame()
    n = 1
    if pages > 1:
        while n < (pages+1):
            URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/data-points/study?page="+str(n)+"&page_size=1000")
            x=requests.get(URL,headers=api_call_headers)
            x=json.loads(x.text)
            verifications = x['_embedded']['items']
            df = pd.DataFrame(verifications, columns=['field_id','field_value','participant_id','updated_on'])
            result = result.append(df, ignore_index=True)
            n = n+1
    elif pages == 1:
        URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/data-points/study?page="+str(n)+"&page_size=1000")
        x=requests.get(URL,headers=api_call_headers)
        x=json.loads(x.text)
        verifications = x['_embedded']['items']
        df = pd.DataFrame(verifications, columns=['field_id','field_value','participant_id','updated_on'])
        result = result.append(df, ignore_index=True)
    return(result)

def fields(api_call_headers):
    URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/field?page_size=1000")
    x=requests.get(URL,headers=api_call_headers)
    # print(x)
    x=json.loads(x.text)
    pages=(x['page_count'])
    result = pd.DataFrame()
    n = 1
    if pages > 1:
        while n < (pages+1):
            URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/field?page="+str(n)+"&page_size=1000")
            x=requests.get(URL,headers=api_call_headers)
            x=json.loads(x.text)
            verifications = x['_embedded']['fields']
            df = pd.DataFrame(verifications, columns=['field_id','parent_id','field_variable_name','field_label'])
            result = result.append(df, ignore_index=True)
            n = n+1
    elif pages == 1:
        URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/field?page="+str(n)+"&page_size=1000")
        x=requests.get(URL,headers=api_call_headers)
        x=json.loads(x.text)
        verifications = x['_embedded']['fields']
        df = pd.DataFrame(verifications, columns=['field_id','parent_id','field_variable_name','field_label'])
        result = result.append(df, ignore_index=True)
    return(result)

def forms(api_call_headers):
    URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/form?page_size=1000")
    x=requests.get(URL,headers=api_call_headers)
    # print(x)
    x=json.loads(x.text)
    pages=(x['page_count'])
    result = pd.DataFrame()
    n = 1
    m = 0
    visits=[]
    if pages > 1:
        while n < (pages+1):
            URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/form?page="+str(n)+"&page_size=1000")
            x=requests.get(URL,headers=api_call_headers)
            x=json.loads(x.text)
            forms = x['_embedded']['forms']
            while m < len(forms):
                visits.append(x['_embedded']['forms'][m]['_embedded']['visit']['visit_id'])
                m = m+1
            df = pd.DataFrame(forms, columns=['form_id','form_name'])
            df['visits'] = visits
            result = result.append(df, ignore_index=True)
            n = n+1
    elif pages == 1:
        URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/form?page="+str(n)+"&page_size=1000")
        x=requests.get(URL,headers=api_call_headers)
        x=json.loads(x.text)
        forms = x['_embedded']['forms']
        while m < len(forms):
            visits.append(x['_embedded']['forms'][m]['_embedded']['visit']['visit_id'])
            m = m+1
        df = pd.DataFrame(forms, columns=['form_id','form_name'])
        df['visits'] = visits
        result = result.append(df, ignore_index=True)
    return(result)

def visits_structure(api_call_headers):
    URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/visit?page_size=1000")
    x=requests.get(URL,headers=api_call_headers)
    # print(x)
    x=json.loads(x.text)
    pages=(x['page_count'])
    result = pd.DataFrame()
    n=1
    if pages > 1:
        while n < (pages+1):
            URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/visit?page="+str(n)+"&page_size=1000")
            x=requests.get(URL,headers=api_call_headers)
            x=json.loads(x.text)
            visits = x['_embedded']['visits']
            df = pd.DataFrame(visits, columns=['visit_id','visit_name'])
            result = result.append(df, ignore_index=True)
            n = n+1
    elif pages == 1:
        URL=("https://"+domain+".castoredc.com/api/study/"+study_id+"/visit?page="+str(n)+"&page_size=1000")
        x=requests.get(URL,headers=api_call_headers)
        x=json.loads(x.text)
        visits = x['_embedded']['visits']
        df = pd.DataFrame(visits, columns=['visit_id','visit_name'])
        result = result.append(df, ignore_index=True)
    return(result)

api_call_headers = get_new_token()
field_result = field_results(api_call_headers)
field_list = fields(api_call_headers)
forms_s = forms(api_call_headers)
visits_s = visits_structure(api_call_headers)

result = pd.merge(field_result,field_list, on = 'field_id' ,how='left')
result = pd.merge(result,forms_s, left_on = 'parent_id', right_on = 'form_id' ,how='left')
result = pd.merge(result,visits_s, left_on = 'visits', right_on = 'visit_id' ,how='left')
# print(field_result)
# print(field_list)
# print(forms_s)
# print(visits_s)
print(result)
result.to_csv('result.csv')