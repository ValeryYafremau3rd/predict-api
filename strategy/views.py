from django.http import HttpResponse
import json
from smtplib import SMTPResponseException
from django.conf import settings
from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId
import redis
import openpyxl

import pymongo
myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
matches = myclient["statistics"]["matches"]
fixtures = myclient["statistics"]["fixtures"]
relations = myclient["statistics"]["relations"]
strategy = myclient["statistics"]["predicts"]
queue = myclient["statistics"]["queue"]
r = redis.Redis(host='host.docker.internal', port=6379, decode_responses=True)


def build_xml():
    return 'hello.xlsx'





def predicts(request, userId):
    return JsonResponse({'data': [{**x, '_id': str(x['_id'])} for x in list(strategy.find({'userId': userId}))]})


def dowload_xml(request, userId):
    response = HttpResponse(content_type='application/vnd.ms-excel')
    response['Content-Disposition'] = 'attachment; filename="Data.xlsx"'

    # create workbook
    wb = openpyxl.Workbook()
    sheet = wb.active

    # stylize header row
    # 'id','title', 'quantity','pub_date'

    c1 = sheet.cell(row = 1, column = 1) 
    c1.value = "id"
    #c1.font = openpyxl.Font(bold=True)

    c2 = sheet.cell(row= 1 , column = 2) 
    c2.value = "title"
    #c2.font = openpyxl.Font(bold=True)

    c3 = sheet.cell(row= 1 , column = 3) 
    c3.value = "quantity"
    #c3.font = openpyxl.Font(bold=True)

    c4 = sheet.cell(row= 1 , column = 4) 
    c4.value = "pub_date"
    #c4.font = openpyxl.Font(bold=True)

    # export data to Excel
    #rows = openpyxl.models.Data.objects.all().values_list('id','category', 'quantity','pub_date',)
    #for row_num, row in enumerate(rows, 1):
    #    # row is just a tuple
    #    for col_num, value in enumerate(row):
    #        c5 = sheet.cell(row=row_num+1, column=col_num+1) 
    #        c5.value = value

    wb.save(response)

    return response

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
