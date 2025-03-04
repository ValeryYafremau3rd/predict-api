from django.conf import settings
from django.http import JsonResponse
from django.template import loader
from bson.objectid import ObjectId
from django.views.decorators.http import require_http_methods
from .services import get_teams, get_leagues
import services.mongodb as db
import pymongo


def teams_in_year(season, league):
    return list(get_teams(league, season))


@require_http_methods(["GET"])
def teams(request, name):
    cur_year_teams = teams_in_year(2024, name)
    prev_year_teams = teams_in_year(2023, name)
    promoted_teams = filter(lambda x: not x in prev_year_teams, cur_year_teams)
    return JsonResponse({'teams': [x['_id'] for x in cur_year_teams], 'promotedTeams': [x['_id'] for x in list(promoted_teams)]})


@require_http_methods(["GET"])
def leagues(request):
    leagues = get_leagues()
    return JsonResponse({'leagues': [x['_id'] for x in list(leagues)]})


@require_http_methods(["GET"])
def match(request):
    homeTeamId = request.parser_context['kwargs']['homeTeamId']
    awayTeamId = request.parser_context['kwargs']['awayTeamId']
    match = db.matches.find({'homeTeam.team.id': homeTeamId, 'awayTeam.team.id': awayTeamId}).sort(
        {'fixture': pymongo.DESCENDING}).limit(1)
    return JsonResponse({'match': [x['_id'] for x in list(match)]})
