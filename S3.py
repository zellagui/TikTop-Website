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

                                            #INITIALISATION OF THE DATABASE
def databaseINIT(today):
    collectionName = 'LayerOne' + today
    cluster = MongoClient("mongodb+srv://Tiktop:2022@cluster0.fstq1.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["Tiktop"]
    collection = db[collectionName]
    return collection

                                            #QUERY DATA FROM THE LAYER ONE DATA
def queryData(collection):
    allVideoMetada = []
    videoMetadata = {'_id': None,  'URL': None}
    dbItems = collection.find({})
    for item in dbItems:
        videoMetadata['_id'] = item['_id']
        videoMetadata['URL'] = item['downloadAddr']
        allVideoMetada.append(deepcopy(videoMetadata))
    return allVideoMetada

                                            #HOST THE MEDIA ON THE S3 STORAGE - BY DAY AS A FOLDER
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

                                            #MODIFY DOWNLOADADDR BY THE NEW HOSTING LINK AND UPDATING IT ON DB
def uploadLayerOne(allVideoMetadata, collection): #uploadLAyerOne(storeS3, databaseInit)
    dbItems = collection.find({})
    for item in dbItems:
        id = item['_id']
        for meta in allVideoMetadata:
            if (id == meta['_id']):
                collection.update_one({'_id' : id},
                              {'$set': { 'downloadAddr' : meta['URL']}})
    return collection

                                            #SEE ITEM ON CONSOLE
def updateDb(collection):
    dbItems = collection.find({})
    for item in dbItems:
        print(item)

                                                    #MAIN
def initS3(today):
    db = databaseINIT(today)
    data = queryData(db)
    store = storeS3(data)
    upload = uploadLayerOne(store, db)
