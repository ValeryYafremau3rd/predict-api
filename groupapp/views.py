
import json
from django.views.decorators.http import require_http_methods
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from bson.objectid import ObjectId
import services.mongodb as db


@require_http_methods(["POST"])
def create(request):
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    body['userId'] = int(request.headers["authorization"])
    res = db.groups.insert_one(body)
    return JsonResponse({'status': 'success'})


@require_http_methods(["DELETE"])
def delete(request, id):
    db.groups.delete_one({'_id': ObjectId(id)})
    all_groups = list(db.groups.find(
        {'userId': int(request.headers["authorization"])}))
    return JsonResponse({'data': json.loads(json.dumps(all_groups, default=str))})


@require_http_methods(["GET"])
def search(request, id):
    group = db.groups.find_one(
        {'_id': ObjectId(id)})
    return JsonResponse({'data': json.loads(json.dumps(group, default=str))})


@require_http_methods(["GET"])
def group_list(request):
    all_groups = list(db.groups.find(
        {'userId': int(request.headers["authorization"])}))
    return JsonResponse({'data': json.loads(json.dumps(all_groups, default=str))})
