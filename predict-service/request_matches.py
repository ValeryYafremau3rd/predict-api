import time
from services.db import fixtures, matches, leagues, teams
from services.matches import get_data


# COPIED
add_league = {
    "$addFields": {
        'league': '$league.id',
        'season': '$league.season'
    }
}


def get_teams(league, season):
    return fixtures.aggregate([
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
        }, {'$sort': {'teams.name': 1}}
    ])


def update_leagues():
    new_leagues = get_data('/leagues')
    leagues.insert_many(new_leagues['response'])


def update_teams(league=39, season=2024):
    new_teams = get_data(f'/teams?league={league}&season={season}')
    teams.insert_many(
        [{**team, 'season': season, 'league': league} for team in new_teams['response']])


def fetch_team_statistics(league=39, season=2024, limit=1, skip=0):
    all_teams = teams.find(
        {'league': league, 'season': season}).skip(skip).limit(limit)
    for team in all_teams:
        teamId = team['team']['id']
        team_statisctics = get_data(
            f'/teams/statistics?league={league}&season={season}&team={teamId}')['response']
        print(team['team']['name'])
        teams.update_one({'team.id': teamId}, {
                         "$set": {'statistics': team_statisctics}})


def update_fixtures(league, season):
    # fetch fixtures
    new_fixtures = get_data(f'/fixtures?league={league}&season={season}')

    # print(new_fixtures)
    if len(new_fixtures['response']):
        fixtures.insert_many(new_fixtures['response'])

        # remove future fixtures
        print(fixtures.delete_many(
            {'league.id': league, 'league.season': season, 'fixture.status.long': {'$ne': 'Match Finished'}}))

        # remove duplicates
        all_fixtures = fixtures.find(
            {'league.id': league, 'league.season': season})
        for fixture in all_fixtures:
            for fixture_2nd in fixtures.find({'league.id': league, 'league.season': season}):
                if fixture_2nd['fixture']['id'] == fixture['fixture']['id'] and fixture_2nd['_id'] != fixture['_id']:
                    fixtures.delete_one({'_id': fixture['_id']})
                    # print(fixture['_id'])


def remove_old_matches(league):
    print('removed matches:')
    all_matches = matches.find({'league': league, 'active': True})
    for match_1st in all_matches:
        for match_2nd in matches.find({'league': league, 'active': True}):
            if match_1st['_id'] != match_2nd['_id']:
                if match_2nd['fixture'] == match_1st['fixture']:
                    matches.delete_one({'_id': match_2nd['_id']})
                    print('fixture duplicate: ' + match_2nd['_id'])
                elif match_2nd['homeTeam']['team']['id'] == match_1st['homeTeam']['team']['id'] and match_2nd['awayTeam']['team']['id'] == match_1st['awayTeam']['team']['id']:
                    if match_2nd['season'] > match_1st['season']:
                        # print(match_2nd['homeTeam']['team']['name'] + ' : ' + match_2nd['awayTeam']
                        #      ['team']['name'] + ' / ' + str(matches.find_one({'_id': match_1st['_id']})['season']))
                        matches.update_one(
                            {'_id': match_1st['_id']}, {"$set": {'active': False}})
                    elif match_1st['season'] > match_2nd['season']:
                        # print(match_2nd['homeTeam']['team']['name'] + ' : ' + match_2nd['awayTeam']
                        #      ['team']['name'] + ' / ' + str(matches.find_one({'_id': match_2nd['_id']})['season']))
                        matches.update_one(
                            {'_id': match_2nd['_id']}, {"$set": {'active': False}})


def remove_relegeted(season=2024):
    cur_season_teams = []
    for league_id in [39, 78, 135, 140, 61]:
        cur_season_teams.extend([x['_id']['id']
                                for x in list(get_teams(league_id, season))])
    matches.update_many({"$and": [
        {'active': True},
        {"$or": [
            {"homeTeam.team.id": {"$nin": cur_season_teams}},
            {"awayTeam.team.id": {"$nin": cur_season_teams}}
        ]}
    ]}, {"$set": {'active': False}})


def fetch_finished_matches(league, season, limit=10, skip=0):
    matches_to_insert = []
    all_fixtures = fixtures.find(
        {'league.id': league, 'league.season': season}).sort({'fixture.id': 1}).skip(skip)
    try:
        for fixture in all_fixtures:
            if fixture['fixture']['id'] == 1223728:  # can't request
                print('fixture 1223728')
            else:
                match = matches.find_one({'fixture': fixture['fixture']['id']})
                if not match:
                    print(fixture['league']['round'] + ' > ' + fixture['teams']
                          ['home']['name'] + ' : ' + fixture['teams']['away']['name'])
                    finished_match = get_data(
                        f'/fixtures/statistics?fixture={fixture["fixture"]["id"]}')
                    matches_to_insert.append(
                        {"homeTeam": finished_match["response"][0], "awayTeam": finished_match["response"][1], "fixture": fixture["fixture"]["id"], "league": fixture["league"]["id"], "season": fixture["league"]["season"], 'time': fixture['fixture']['timestamp'], 'active': True})
                if len(matches_to_insert) >= limit:
                    break
    finally:
        if len(matches_to_insert):
            matches.insert_many(matches_to_insert)


# matches.update_many({}, {"$set": {'active': True}})

#update_leagues()
#update_teams(78, 2024)
#fetch_team_statistics(league=61, season=2024, limit=10, skip=10)

#update_fixtures(39, 2024)
#update_fixtures(78, 2024)
#update_fixtures(140, 2024)
#update_fixtures(61, 2024)

#while True:
#fetch_finished_matches(61, 2025, 10, 0)
#    time.sleep(61)
remove_old_matches(61)
remove_relegeted()
#for fixture in fixtures.find():
#    print(fixture['fixture']['id'])
#    matches.update_one({'fixture': fixture['fixture']['id']}, {
#                         "$set": {'time': fixture['fixture']['timestamp']}})


#for league_id in [135]:
#   remove_old_matches(league_id)
# 39 pl +
# 78 bl +
# 135 sa +
# 140 p +
# 61 l1
