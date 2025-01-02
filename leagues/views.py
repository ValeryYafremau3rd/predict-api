from django.conf import settings
from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId

import pymongo
connect_string = 'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'

my_client = pymongo.MongoClient(connect_string)

# First define the database name
dbname = my_client['statistics']
collection_name = dbname["fixtures"]

group_by_league = {
    "$group": {
        "_id": "$league",
    }
}
add_league = {
    "$addFields": {
        'league': '$league.name',
        'season': '$league.season'
    }
}

pipeline = [
    add_league,
    group_by_league
]
teams_pipeline = [
    add_league
]


def teams_in_year(season, league):
    return list(collection_name.aggregate([
        add_league, {
            "$match": {
                "league": league,
                "season": season,
            }
        }, {
            "$addFields": {
                'homeTeam': '$teams.home.name',
            }
        },
        {
            "$group": {
                "_id": {
                    'id': "$teams.home.id",
                    'name': '$teams.home.name'
                }
            }
        }
    ]))


def teams(request, name):
    cur_year_teams = teams_in_year(2024, name)
    prev_year_teams = teams_in_year(2023, name)
    promoted_teams = filter(lambda x: not x in prev_year_teams, cur_year_teams)
    return JsonResponse({'teams': [x['_id'] for x in cur_year_teams], 'promotedTeams': [x['_id'] for x in list(promoted_teams)]})


def leagues(request):
    leagues = collection_name.aggregate(pipeline)
    return JsonResponse({'leagues': [x['_id'] for x in list(leagues)]})
