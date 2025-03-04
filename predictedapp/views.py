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
    response = HttpResponse(content_type='application/vnd.ms-excel')
    response['Content-Disposition'] = 'attachment; filename="Data.xlsx"'
    generate_xl_file(db.strategy, int(request.headers["authorization"])).save(response)

    return response


@require_http_methods(["DELETE"])
def delete(request, matchId):
    match = db.strategy.delete_one({'_id': ObjectId(matchId)})
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(db.strategy.find({'userId': int(request.headers["authorization"])}))]})
