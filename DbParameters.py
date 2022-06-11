from datetime import date
import pymongo
from pymongo import MongoClient
import urllib.request


date = date.today()
#today = date.strftime("%d-%m-%Y")
today = '10-06-2022'
collectionName = 'LayerOne' + today
cluster = MongoClient("ENTER CLUSTER INFO")
db = cluster["Tiktop"]
collection = db[collectionName]

results = collection.find({})
collection.delete_many({})


#count = 0
#for x in results:
#  file = urllib.request.urlopen(x['downloadAddr'])
#   #count = file.length + count
#    count = count +1
#print(count)
