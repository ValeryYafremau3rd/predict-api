from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId

import pymongo
connect_string = 'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'

from django.conf import settings
my_client = pymongo.MongoClient(connect_string)

# First define the database name
dbname = my_client['statistics']
collection_name = dbname["matches"]


def match(request, id):
  match1 = collection_name.find_one({'_id': ObjectId(id)}, {'_id': 0})
  print(match1)
  return JsonResponse(match1)
