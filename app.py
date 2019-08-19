from ldap3 import Server, Connection, ALL, ObjectDef, Reader
import os
import json
import time
import traceback
import requests
from queue import Queue
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

username = os.environ['SERVICE_ACCOUNT_USERNAME']
password = os.environ['SERVICE_ACCOUNT_PASSWORD']

def create_base_tables():
    ldapGroups = { 'ldapGroups': [] }
    for key, value in os.environ.items():
        if 'DEFAULT_GROUP_DN' in key:
            ldap_group = ldapGroups['ldapGroups'].append(value)
    requests.post(url = f'{os.environ["MONGODB_URL"]}/ldapGroups', json = ldapGroups, verify=False)
    return
    
def query_user(conn, obj_user, user, group, tempList):
    try:
        r = Reader(conn, obj_user, user)
        result = r.search_subtree()
        ldapUserPN = str(result[0]['userPrincipalName']).lower()
        email = str(result[0]['mail']).lower()
        ldapUserDisplayName = str(result[0]['displayName'])
        ldapUsername = f'{ldapUserPN.split("@")[-1].split(".")[0]}\\{ldapUserPN.split("@")[0]}'
        ldapUserNoDomain = ldapUserPN.split('@')[0]
        ldapGroupDN = group
        ldapGroupCN = group.split(',')[0].strip('CN=')
        tempList['ldapUsers'].append(
            { 
                'ldapUsername': ldapUsername,
                'ldapUserPN': ldapUserPN,
                'ldapUserNoDomain': ldapUserNoDomain,
                'ldapUserDisplayName': ldapUserDisplayName,
                'email': email,
                'ldapGroupDN': ldapGroupDN,
                'ldapGroupCN': ldapGroupCN
            }
        )
        print(f'Adding {email} to tempList')
        return
    except Exception as e:
        print(f'Error with user: {user}')
        print(str(e))
        

while True:
    print('!\n!\n!\nStarting LDAP Queries\n!\n!\n!')
    server = Server(os.environ['AD_SERVER'], get_info=ALL, port=3269, use_ssl=True)
    with Connection(server, user=username, password=password, auto_bind=True) as conn:
        obj_group = ObjectDef('group', conn)
        obj_user = ObjectDef('user', conn)
        requests.delete(url = f'{os.environ["MONGODB_URL"]}/collections?collection=ldapGroups', verify=False)
        create_base_tables()
        tempList = { 'ldapUsers': [] }
        getGroups = requests.get(url = f'{os.environ["MONGODB_URL"]}/ldapGroups', verify=False).json()
        for item in getGroups:
            if 'ldapGroups' in item:
                groupQuery = item['ldapGroups']
        groupQueue = Queue()
        for group in groupQuery:
            groupQueue.put({'nestedGroup': group, 'targetGroup': group})
        while not groupQueue.empty():
            group = groupQueue.get()
            print(f'!\n!\n!\nQuerying Group: {group["nestedGroup"]}\n!\n!\n!')
            try:
                r = Reader(conn, obj_group, group['nestedGroup'])
                search_for_group = r.search()
                results = search_for_group[0]['member']
            except IndexError:
                print(f'Error with group: {group["nestedGroup"]}')
                results = []
            for user in results:
                print(user)
                if any(text in user for text in ['.UG','.GG','.LG','Exchange Dist Groups']):
                    groupQueue.put({'nestedGroup':user,'targetGroup':group['targetGroup']})
                    print(f'!\n!\n!\nAdding Nested Group to Queue: {user}\n!\n!\n!')
                else:
                    query_user(conn, obj_user, user, group['targetGroup'], tempList)
    try:
        print('!\n!\n!\nInserting Users Into Mongo Database From TempList\n!\n!\n!')
        requests.delete(url = f'{os.environ["MONGODB_URL"]}/collections?collection=ldapUsers', verify=False)
        requests.post(url = f'{os.environ["MONGODB_URL"]}/ldapUsers', json = tempList, verify=False)
        print('!\n!\n!\nCompleted Inserting Users Into Database From TempList\n!\n!\n!')
    except Exception as e:
        print('failed post')
    time.sleep(3600)
