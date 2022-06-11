import json
import pymongo
from pymongo import MongoClient
from datetime import date
import requests
from copy import deepcopy


def lambda_handler(event, context):
    init(25)


date = date.today()
today = date.strftime("%d-%m-%Y")

id = '1653152643912710'
name = '#tiktokmademebuythis'

all_video_data = []
sorted_all_video_data = []


### Init the database with today's date
def databaseINIT(today):
    collectionName = 'LayerOne' + today
    cluster = MongoClient("ENTER ")
    db = cluster["Tiktop"]
    collection = db[collectionName]
    return collection

    ### Get the first connexion to the API
def first_connexion():
    headers = {
        'X-API-KEY': 'ENTER API KEY',
        'accept': 'application/json',
        'country': 'us',
    }

    params = {
        'id': id, #Modify this value [id] to get another Hashtag feed - The link between Hashtags and ID is stored in the #_data_file
    }

    response = requests.get('https://api.tikapi.io/public/hashtag', params=params, headers=headers)
    data = response.json()
    return data

    ### Selecting Data to add at the Database
def get_data_content(data):
    dict = {'secUid': None,
            'username': None,
            'verfified': None,
            'engagement': None,
            'createdTime': None,
            'desc': None,
            '_id': None,
            'authorStats':{
                'authStats_diggCount': None,
                'followerCount': None,
                'followingCount': None,
                'heart': None,
                'videoCount': None },
            'stats': {'commentCount': None,
                      'diggCount': None,
                      'playCount': None,
                      'shareCount': None },
            'music': {'authorName':None,
                      'duration': None,
                      'original': None,
                      'title': None,
                      'id': None,
                      'playUrl': None},
            'downloadAddr':None,
            'duration': None
            }

    list_dict = [] #for each items there will be a dict like above stored in this list

    for info in data:
        cursor = data['cursor']
        hasNext = data['hasMore']

    itemlist = data['itemList']

    for item in itemlist:
        try:
            dict['secUid'] = item['author']['secUid']
            dict['username'] = item['author']['uniqueId']
            dict['verfified'] = item['author']['verified']


            dict['authorStats']['diggCount'] = item['authorStats']['diggCount']
            dict['authorStats']['followerCount'] = item['authorStats']['followerCount']
            dict['authorStats']['followingCount'] = item['authorStats']['followingCount']
            dict['authorStats']['heart'] = item['authorStats']['heart']
            dict['authorStats']['videoCount'] = item['authorStats']['videoCount']

            dict['createdTime'] = item['createTime']
            dict['desc'] = item['desc']
            dict['_id'] = item['id']

            dict['stats']['commentCount'] = item['stats']['commentCount']
            dict['stats']['diggCount'] = item['stats']['diggCount']
            dict['stats']['playCount'] = item['stats']['playCount']
            dict['stats']['shareCount'] = item['stats']['shareCount']

            dict['music']['authorName'] = item['music']['authorName']
            dict['music']['duration'] = item['music']['duration']
            dict['music']['title'] = item['music']['title']
            dict['music']['id'] = item['music']['id']
            dict['music']['playUrl'] = item['music']['playUrl']

            dict['duration'] = item['video']['duration']
            dict['downloadAddr'] = item['video']['downloadAddr']

            nb_like =  dict['stats']['diggCount']
            nb_comment = dict['stats']['commentCount']
            nb_share = dict['stats']['shareCount']
            nb_view = item['stats']['playCount']

        except:
            print('The download error happenend')
            continue

        #Engagement function to give a score on 100%
        dict['engagement'] = calculate_engagement(nb_like, nb_comment, nb_share, nb_view)

        #print(dict)
        list_dict.append(deepcopy(dict))

    for i in list_dict:
        all_video_data.append(i)

    #get the final List[video_data] sorted by engagement rate
    sortedd = sorted(all_video_data, key=lambda d: d['engagement'], reverse=True)

    print(str(len(sorted_all_video_data)) + ' allvidzdata')


    for i in sortedd:
        sorted_all_video_data.append(i)

    remove_dup(sorted_all_video_data)

    print(str(cursor) + ' = cursor ')


    #print(sorted_all_video_data)

    #making there is more element
    if(hasNext == True):
        return cursor
    else:
        print("No more page found")

        ### Each iteration mean a new API call
def connexion_loop(cursor, NB_PAGE):
    #Number of page returned [By 30 items in each page]. Already one called (get_data_content), so Add 1.
    for i in range(NB_PAGE):
        headers = {
            'X-API-KEY': 'ENTER API KEY',
            'accept': 'application/json',
        }
        params = {
            'id': id, #Modify this value [id] to get another Hashtag feed - The link between Hashtags and ID is stored in the #_data_file
            'count': '30',
            'cursor': cursor,
        }
        response = requests.get('https://api.tikapi.io/public/hashtag', params=params, headers=headers)
        data = response.json()

        if cursor == None:
            print('first and cursor')
            pass
        else:
            cursor = get_data_content(data) # call again function with the data / get new cursor
    return cursor


    ### Remove duplicated  value in a list
def remove_dup(a):
    i = 0
    while i < len(a):
        j = i + 1
        while j < len(a):
            if a[i] == a[j]:
                del a[j]
            else:
                j += 1
        i += 1

        ###Calculate the engagement of the data
def calculate_engagement(nb_like, nb_comment, nb_share, nb_view):
    if(nb_comment < 30 or nb_share < 10 or nb_like < 150):
        # remember that 0.101 are video with less than 30 comments
        return 0.101
    else:
        engagement = round(nb_like + nb_comment + nb_share) / nb_view * 100
        return round(engagement)


        ###Selected the data choosen data and append it to DB
def selectData(sorted_all_video_data, collection, max_items):
    count = 0
    for video in sorted_all_video_data:
        collection.insert_one(video)
        if(count == max_items):
            break
    count = count + 1




    ###MAIN
# The first connexion to the API
def init(Nb_page):
    collection = databaseINIT(today)
    data = first_connexion()
    print('First connexion OK!')

    get_data_content(data)
    print('Data access OK!')

    for info in data: #data from the first connexion
        cursor = data['cursor'] #get access to the first next cursor

    print('connexion loop starting')
    connexion_loop(cursor, Nb_page)

    selectData(sorted_all_video_data, collection, 120)

    print('Connected!')
    print(str(len(sorted_all_video_data)) + 'Tiktoks selected')
