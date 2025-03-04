from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId
from django.views.decorators.http import require_http_methods
import services.mongodb as db


@require_http_methods(["GET"])
def match(request, id):
  match1 = db.fixtures.find_one({'_id': ObjectId(id)}, {'_id': 0})
  return JsonResponse(match1)
