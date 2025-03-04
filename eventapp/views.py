
import json
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from bson.objectid import ObjectId
import services.mongodb as db


@require_http_methods(["POST"])
def create(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(request.headers["authorization"])
    res = db.custom_odds.insert_one(body)
    return JsonResponse({'status': 'success'})


@require_http_methods(["DELETE"])
def delete(request, id):
    db.custom_odds.delete_one({'_id': ObjectId(id)})
    allOdds = list(db.custom_odds.find(
        {'userId': int(request.headers["authorization"])}, projection={"name": 1, "_id": 1}))
    return JsonResponse({'data': json.loads(json.dumps(allOdds, default=str))})


@require_http_methods(["GET"])
def odd(request, id):
    odd = db.custom_odds.find_one({'_id': ObjectId(id)})
    return JsonResponse({'data': json.loads(json.dumps(odd, default=str))})


@require_http_methods(["POST"])
def edit(request, id):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(request.headers["authorization"])
    odd = db.custom_odds.update_one({'_id': ObjectId(id)}, {"$set": body})
    return JsonResponse({'status': 'success'})


@require_http_methods(["GET"])
def event_list(request):
    allOdds = list(db.custom_odds.find(
        {'userId': int(request.headers["authorization"])}, projection={"name": 1, "_id": 1}))
    return JsonResponse({'data': json.loads(json.dumps(allOdds, default=str))})


@require_http_methods(["GET"])
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
