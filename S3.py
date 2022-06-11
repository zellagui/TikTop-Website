from S3secret import access_key, secret_access_key
from datetime import date

import requests
import boto3
import pymongo
from pymongo import MongoClient
from copy import deepcopy

date = date.today()
today = date.strftime("%d-%m-%Y")
client = boto3.client('s3',
                            aws_access_key_id = access_key,
                            aws_secret_access_key = secret_access_key)

def databaseINIT(today):
    collectionName = 'LayerOne' + today
    cluster = MongoClient("ENTER CLUSTER INFO")
    db = cluster["Tiktop"]
    collection = db[collectionName]
    return collection

def queryData(collection):
    allVideoMetada = []
    videoMetadata = {'_id': None,  'URL': None}
    dbItems = collection.find({})
    for item in dbItems:
        videoMetadata['_id'] = item['_id']
        videoMetadata['URL'] = item['downloadAddr']
        allVideoMetada.append(deepcopy(videoMetadata))
    return allVideoMetada

def storeS3(allVideoMetadata):
    for item in allVideoMetadata:
        url = item['URL']
        id = item['_id']
        request = requests.get(url, stream=True)
        upload_file_bucket = 'downloadaddr'
        upload_file_key = str(today) + '/' +  str(id) +'.mp4'
        client.upload_fileobj(request.raw, upload_file_bucket, upload_file_key)
        item['URL'] = 'https://downloadaddr.s3.amazonaws.com/' + today + '/' + id + '.mp4'
    return allVideoMetadata

def uploadLayerOne(allVideoMetadata, collection): #uploadLAyerOne(storeS3, databaseInit)
    dbItems = collection.find({})
    for item in dbItems:
        id = item['_id']
        for meta in allVideoMetadata:
            if (id == meta['_id']):
                item['downloadAddr'] = meta['URL']
        collection.delete_one(item)
        print(item)
    return collection


#MAIN
db = databaseINIT(today)
data = queryData(db)
store = storeS3(data)
upload = uploadLayerOne(store, db)
