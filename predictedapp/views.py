import json
from django.http import HttpResponse
from django.conf import settings
from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId
from django.views.decorators.http import require_http_methods
from .services import generate_xl_file
import services.mongodb as db


@require_http_methods(["GET"])
def predicts(request):
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(db.strategy.find({'userId': int(request.headers["authorization"])}))]})


@require_http_methods(["POST"])
def dowload(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    response = HttpResponse(content_type='application/vnd.ms-excel')
    response['Content-Disposition'] = 'attachment; filename="Data.xlsx"'
    generate_xl_file(db.strategy, int(request.headers["authorization"]), body['leagues'], body['groups']).save(response)

    return response

@require_http_methods(["POST"])
def numberOfMatches(request):
    userId = int(request.headers["authorization"])
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    query = {
        "userId": userId,
        "league": {"$in": body['leagues']},
        "$or": [{f"odds.{k}": {"$exists": True}} for k in body['groups']]
    }
    numberOfPredicts = db.strategy.count_documents(query)
    return JsonResponse({'data': numberOfPredicts})

@require_http_methods(["DELETE"])
def delete(request, matchId):
    match = db.strategy.delete_one({'_id': ObjectId(matchId)})
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(db.strategy.find({'userId': int(request.headers["authorization"])}))]})

@require_http_methods(["DELETE"])
def delete_all(request):
    db.strategy.delete_many({'userId': int(request.headers["authorization"])})
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(db.strategy.find({'userId': int(request.headers["authorization"])}))]})
