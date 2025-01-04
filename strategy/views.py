import json
from django.conf import settings
from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId
import redis

import pymongo
myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
matches = myclient["statistics"]["matches"]
fixtures = myclient["statistics"]["fixtures"]
relations = myclient["statistics"]["relations"]
strategy = myclient["statistics"]["predicts"]
queue = myclient["statistics"]["queue"]
r = redis.Redis(host='host.docker.internal', port=6379, decode_responses=True)


def predicts(request, userId):
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(strategy.find({'userId': userId}))]})


def add_to_queue(request, userId):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(body['userId'])
    #res = queue.update_one({'user_id': body['userId']}, {
    #    '$push': {'queue': [body['homeTeam'], body['awayTeam']]}}, upsert=True)
    res = queue.insert_one(body)
    print(res.inserted_id )
    r.publish('task', json.dumps({'task_id': str(res.inserted_id)}))
    return JsonResponse({'data': json.loads(json.dumps(list(queue.find({'userId': int(userId)})), default=str))})


def get_queue(request, userId):
    print(list(queue.find({'userId': userId}, projection={'_id': 0})))
    return JsonResponse({'data': json.loads(json.dumps(list(queue.find({'userId': userId})), default=str))})


def delete_from_queue(request, matchId):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    match = queue.delete_one({'_id': ObjectId(matchId)})
    return JsonResponse({'data': json.loads(json.dumps(list(queue.find({'userId': int(body['userId'])})), default=str))})
    # return JsonResponse({'data': [x for x in list(strategy.find(projection={'_id': 0}))]})


def delete_from_results(request, matchId):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    match = strategy.delete_one({'_id': ObjectId(matchId)})
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(strategy.find({'userId': int(body['userId'])}))]})
