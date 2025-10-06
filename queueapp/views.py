import json
from django.http import JsonResponse
from bson.objectid import ObjectId
from django.views.decorators.http import require_http_methods
import services.mongodb as db
from services.redis import r as redis


@require_http_methods(["POST"])
def add_to_queue(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(request.headers["authorization"])
    # res = queue.update_one({'user_id': body['userId']}, {
    #    '$push': {'queue': [body['homeTeam'], body['awayTeam']]}}, upsert=True)
    res = db.queue.insert_one(body)
    redis.lpush('task_queue', json.dumps({'task_id': str(res.inserted_id)}))
    return JsonResponse({'data': json.loads(json.dumps(list(db.queue.find({'userId': int(request.headers["authorization"])})), default=str))})


@require_http_methods(["GET"])
def get_queue(request):
    return JsonResponse({'data': json.loads(json.dumps(list(db.queue.find({'userId': int(request.headers["authorization"])})), default=str))})

@require_http_methods(["DELETE"])
def delete_all_from_queue(request):
    db.queue.delete_many({'userId': int(request.headers["authorization"])})
    return JsonResponse({'data': json.loads(json.dumps(list(db.queue.find({'userId': int(request.headers["authorization"])})), default=str))})

@require_http_methods(["DELETE"])
def delete_from_queue(request, matchId):
    match = db.queue.delete_one({'_id': ObjectId(matchId)})
    return JsonResponse({'data': json.loads(json.dumps(list(db.queue.find({'userId': int(request.headers["authorization"])})), default=str))})
