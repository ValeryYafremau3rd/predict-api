
import json
from django.http import JsonResponse
import pymongo
from bson.objectid import ObjectId
myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
custom_odds = myclient["statistics"]["custom-odds"]
groups = myclient["statistics"]["groups"]


def create(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(body['userId'])
    print(body)
    res = custom_odds.insert_one(body)
    return JsonResponse({'status': 'success'})

def groupCreate(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(body['userId'])
    print(body)
    res = groups.insert_one(body)
    return JsonResponse({'status': 'success'})


def delete(request, id):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    custom_odds.delete_one({'_id': ObjectId(id)})
    print( body['userId'])
    allOdds = list(custom_odds.find(
        {'userId': int(body['userId'])}, projection={"name": 1, "_id": 1}))
    return JsonResponse({'data': json.loads(json.dumps(allOdds, default=str))})

def groupDelete(request, id):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    groups.delete_one({'_id': ObjectId(id)})
    allGroups = list(groups.find(
        {'userId': int(body['userId'])}))
    return JsonResponse({'data': json.loads(json.dumps(allGroups, default=str))})

def getGroup(request, id):
    group = groups.find_one(
        {'_id': ObjectId(id)})
    return JsonResponse({'data': json.loads(json.dumps(group, default=str))})


def odd(request, id):
    odd = custom_odds.find_one({'_id': ObjectId(id)})
    return JsonResponse({'data': json.loads(json.dumps(odd, default=str))})


def edit(request, id):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(body['userId'])
    odd = custom_odds.update_one({'_id': ObjectId(id)}, {"$set": body})
    return JsonResponse({'status': 'success'})


def oddList(request, userId):
    print(list(custom_odds.find(
        {'userId': userId}, projection={"name": 1, "_id": 1})))
    allOdds = list(custom_odds.find(
        {'userId': userId}, projection={"name": 1, "_id": 1}))
    return JsonResponse({'data': json.loads(json.dumps(allOdds, default=str))})

def groupList(request, userId):
    allGroups = list(groups.find(
        {'userId': userId}))
    return JsonResponse({'data': json.loads(json.dumps(allGroups, default=str))})


def operations(request):
    operators = ['and', 'or', '=', '>', '>=', '<', '<=', '!=', '+', '-']
    types = ['value', 'stat']
    preffixes = ['Home Team', 'Away Team']
    stats = [
        'Shots on Goal',
        'Shots off Goal',
        'Blocked Shots',
        'Shots insidebox',
        'Shots outsidebox',
        'Fouls',
        'Yellow Cards',
        'Red Cards',
        'Corner Kicks',
        'Offsides',
        'Ball Possession',
        'Goalkeeper Saves',
        'Total passes',
        'Passes accurate',
        'Passes %',
        'expected_goals',
        'Half Time Goals',
        'Goals',
    ]
    return JsonResponse({'operators': operators, 'types': types, 'preffixes': preffixes, 'stats': stats})
