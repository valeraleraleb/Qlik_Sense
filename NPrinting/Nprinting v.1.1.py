import ssl
import os
import websocket
from websocket import create_connection
import json
import requests
from requests_ntlm import HttpNtlmAuth
from requests.auth import HTTPBasicAuth
import csv
import random
import string
import datetime
import uuid
import shutil

requests.packages.urllib3.disable_warnings()

# ============================================================

QSdocID = '4672caff-2493-4859-bdd5-669f6f636167'
QSpageName = 'ea744dce-496f-4f99-b56c-6f1d01c36044' # keep empty if need to load all pages (name or id)
# Выгрузка для ИЦ
QSbookmarkId = 'e5031d69-9d28-47a7-81f3-0abd2efc8e7a' # id of bookmark from sigle config dev hub

# ============================================================

gv_mainSaveDir = "C:\Qlik Server Space\Extract\CP\SentToSharePoint"
# do you need to save to sharepoint?
isSaveToSP = False 
gv_pathToSharePoing = r"\\department.[host].ru@SSL\DavWWWRoot\sites\qlik\DocLib2\QS - список документов"
# r"\\department.nipigas.ru@SSL\DavWWWRoot\sites\qlik\Tst"
print("Temp folder: " +gv_mainSaveDir)
print("Target folder: " + gv_pathToSharePoing)
print("Save to target folder: " + str(isSaveToSP))
# ============================================================

class QlikEngine():
    # Initialize websocket
    def __init__(self,host,cert_path,user_directory,user_id):
        self.host = host
        self.cert_path = cert_path
        self.user_directory = user_directory
        self.user_id = user_id
        self.xrf = self.__set_xrf__()
        self.handle = -1
        self.headers = dict()
        self.cookies = dict()
        self.session = requests.session()
        self.__get__()
        self.__webSocketConnect__()

    
    #генерирует уникальный 16 значный идентификатор 
    def __set_xrf__(self):
        lv_chars = string.ascii_letters + string.digits
        return ''.join(random.sample(lv_chars, 16))

    def __get__(self):
        self.headers = {
            "X-Qlik-XrfKey" : self.xrf
            , "X-Qlik-User" : "UserDirectory={};userId={}".format(self.user_directory, self.user_id)
            , "Content-Type": "application/json"
        }

        
        lo_path = os.path.join(self.cert_path,'client.pem')
        lo_key = os.path.join(self.cert_path,'client_key.pem')
        self.session.cert = (lo_path,lo_key)
                
        lo_resp = self.session.get(
           'https://{0}:4242/{1}?xrfkey={2}'.format(self.host, 'qrs/app/full', self.xrf)
            , headers = self.headers
            , verify  = False
            , cert=self.session.cert
        )

        # получаем куки сессии
        self.cookies = self.session.cookies.get_dict()
        
        print(lo_resp)
#         print(self.cookies)
        
#         if lo_resp.status_code != 200:
#             # Returns an error if something went wrong.
#             raise ApiError('GET /qrs/app/full {}'.format(lo_resp.status_code))
# #         for app in lo_resp.json():
# #             print('{} {}'.format(app['id'], app['name']))
            
        

    def __webSocketConnect__(self, iv_boardGuid = ''):
        if (len (iv_boardGuid) == 0):
            socketUrl = "wss://{}:4747/app/".format(self.host)
        else:
            socketUrl = "wss://{}:4747/app/{}".format(self.host, iv_boardGuid)
        
        cert = {
                "cert_reqs":ssl.CERT_NONE,
                'ca_certs':os.path.join(self.cert_path,'root.pem'),
                'certfile':os.path.join(self.cert_path,'client.pem'),
                'keyfile':os.path.join(self.cert_path,'client_key.pem')
        }
        self.headers = {
                'X-Qlik-User':f'UserDirectory={self.user_directory};'
                              f'UserId={self.user_id}'
        }
        
        try:
            self.socketUrl = socketUrl
            self.ws = create_connection(
                socketUrl, 
                sslopt = cert, 
                header = self.headers 
                )
            self.result = self.ws.recv()
            result = json.loads(self.result)
            print(result)
            self.sessionState = result.get("params").get("qSessionState")
            if  self.sessionState == 'SESSION_CREATED':
                self.sessionCreated = True
            else:
                self.sessionCreated = False    
        except:
            self.result = json.dumps({
                "params": {"qSessionState":"error"}
            })
            self.sessionCreated = False 
            print("error while websocket opening") 
            
    def getStreams(self):
        self.ws.send(json.dumps({
            "method": "GetStreamList",
            "handle": -1,
            "params": []
        }))
                
        result = self.ws.recv()
        
        data = json.loads(result)
        lo_resJson = data['result']['qStreamList'] #возвращаем список потоков с параметрами
        
        lo_dictionary = {}

        #цикл, где ключ - название потока, значение - id потока
        for i in range (0, len(lo_resJson)):
            lo_dictionary.update({lo_resJson[i]['qName']:lo_resJson[i]['qId']}) #заполняем список пользователю для выбора
        
        return lo_dictionary
    
    def getDocList(self, iv_streamGuid = ''):
        self.ws.send(json.dumps({
            "handle": -1,
            "method": "GetDocList",
            "params": [],
            "outKey": -1
        }))

        result = self.ws.recv()
 
        data = json.loads(result)
#         print(data)
        documentsList = data["result"]
        return documentsList
        documents = []

        for doc in documentsList.get("qDocList"):
            documentMeta = doc.get("qMeta")
            
            if documentMeta.get("published"):
                stream = documentMeta["stream"]["name"]
                streamId = documentMeta["stream"]["id"]
            else:
                stream = None
                streamId = None
            
            document = {
                "docId": doc.get("qDocId"),
                "docName":doc.get("qDocName"),
                "qvfSize": doc.get("qFileSize"),
                "createdDate":documentMeta.get("createdDate"),
                "modifiedDate":documentMeta.get("modifiedDate"),
                "lastReloadTime":documentMeta.get("qLastReloadTime"),
                "published":documentMeta.get("published"),
                "publishTime":documentMeta.get("publishTime"),
                "stream":stream,
                "streamId": streamId
            }    
            if iv_streamGuid == streamId or iv_streamGuid == '':
                documents.append(document)                
        return documents

    def openDoc(self, docId):
        self.__del__()
        self.__webSocketConnect__(docId)
        
        self.ws.send(json.dumps({
            "method": "OpenDoc",
            "handle": -1,
            "params": [
                docId
            ]
#             ,"outKey": -1
        }))
        result = self.ws.recv()
        data = json.loads(result)
        return data
    
    def applyBookmark(self, bookmarkId, handle):
        self.ws.send(json.dumps({
            "method": "ApplyBookmark",
            "handle": handle,
            "params": {
                "qId": bookmarkId
            }
#             ,"outKey": -1
        }))
        result = self.ws.recv()
        data = json.loads(result)
        return data

    def getSheetsObject(self,handle): 
        self.ws.send(json.dumps({
            "method": "CreateSessionObject",
            "handle": handle,
            "params": [
                {
                    "qInfo": {
                        "qType": "SheetList"
                    },
                    "qAppObjectListDef": {
                        "qType": "sheet",
                        "qData": {
                            "title": "/qMetaDef/title",
                            "description": "/qMetaDef/description",
                            "thumbnail": "/thumbnail",
                            "cells": "/cells",
                            "rank": "/rank",
                            "columns": "/columns",
                            "rows": "/rows"
                        }
                    }
                }
            ],
            "outKey": -1
        }))
        result = self.ws.recv()
        sheetObjects = json.loads(result)
        sheetsHandle = sheetObjects['result']['qReturn']['qHandle']
        self.ws.send(json.dumps(
            {
            "method": "GetLayout",
            "handle": sheetsHandle,
            "params": [],
            "outKey": -1
            }
        ))
        result = self.ws.recv()
        data = json.loads(result)
        return data

    def getLayout(self,handle):
        self.ws.send(json.dumps(
            {
            "method": "GetLayout",
            "handle": handle,
            "params": [],
            "outKey": -1
            }
        ))
        result = self.ws.recv()
        data = json.loads(result)
        return data

    def getObject(self,handle,objectId):
        self.ws.send(json.dumps(
            {
                "handle": handle,
                "method": "GetObject",
                "params": {
                    "qId": objectId
                }
            }
        ))
        result = self.ws.recv()
        data = json.loads(result)
        return data

    def exportData(self,handle,exportFormat):
        self.ws.send(json.dumps(
            {
                "method": "ExportData",
                "handle": handle,
                "params": {
                    "qFileType": exportFormat,
                    "qPath": "",
                    "qFileName": "",
                    "qExportState": 0
                }
            }
        ))
        result = self.ws.recv()
#         return result
        data = json.loads(result)
        return data
            
    #Object destroyer       
    def __del__(self):
        if self.ws.connected:
            self.ws.close()
#         self.ws.close()

# =================================================================

if 'qlikApi' in locals():
    qlikApi.__del__()
qlikApi = QlikEngine('s201as-cn1.[host].local', r'C:\QLIK\Certificates\Qlik\_new\.Local Certificates', 'internal', 'sa_engine')

# =================================================================

# list with paths
lo_pathToSent = list()

# full list of docs
lo_fullListDocs = qlikApi.getDocList(QSdocID)
# lo_fullListDocs
# 
# new conn to doc
lo_docOne = qlikApi.openDoc(QSdocID)
#lo_docOne
# 
# Apply bookmark QSbookmarkId
if len(QSbookmarkId) > 0:
    bookmarkInfo = qlikApi.applyBookmark(QSbookmarkId, lo_docOne['result']['qReturn']['qHandle'])
# 
# get sheets in app(doc)
lo_sheetList = qlikApi.getSheetsObject(lo_docOne['result']['qReturn']['qHandle'])
lo_obj_list = lo_sheetList['result']['qLayout']['qAppObjectList']['qItems']
#lo_sheetList

# 
for page in lo_obj_list:
    # get object
    #if you need just one page
    if len(QSpageName) > 0:
        if QSpageName != page['qMeta']['title'] and QSpageName != page['qInfo']['qId']:
            # print(page['qMeta']['title'])
            # print(page['qInfo']['qId'])
            continue
    # TODO: you have to recive in cycle and append to dict
    lo_oneSh = qlikApi.getObject(lo_docOne['result']['qReturn']['qHandle'], page['qInfo']['qId'])
    lv_nameOfPage = page['qMeta']['title']
    print(lv_nameOfPage)
    # 
    # layout of objects name
    lo_objNames = qlikApi.getLayout(lo_oneSh['result']['qReturn']['qHandle'])
    # 
    # Export for sheet
    obb = qlikApi.exportData(lo_oneSh['result']['qReturn']['qHandle'], 2)
    # obb
    # TODO write relocate file, rename and publish
    temp_url = obb['result']['qUrl']
    
    if 'tempcontent' in temp_url and '?' in temp_url:
        temp_url = temp_url.split('?')[0]
        path = r"C:\ProgramData\Qlik\Sense\Repository\TempContent" + temp_url[temp_url.find('/', 1):].replace(r'/','\\')
        
        if os.path.exists(path):
            shutil.copy2(
                path, 
                os.path.join(gv_mainSaveDir, lv_nameOfPage + ".xlsx")
            )
            lo_pathToSent.append(os.path.join(gv_mainSaveDir, lv_nameOfPage + ".xlsx"))
            if isSaveToSP:
                shutil.copy2(
                    os.path.join(gv_mainSaveDir, lv_nameOfPage + ".xlsx"), 
                    os.path.join(gv_pathToSharePoing, lv_nameOfPage + ".xlsx")
                )
                lo_pathToSent.append(os.path.join(gv_pathToSharePoing, lv_nameOfPage + ".xlsx"))

print(lo_pathToSent)
