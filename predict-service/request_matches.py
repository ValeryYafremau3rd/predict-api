import time
from services.db import fixtures, matches, leagues
from services.matches import get_data


def update_leagues():
    new_leagues = get_data('/leagues')
    leagues.insert_many(new_leagues['response'])


def update_features(league, season):
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
    all_matches = matches.find({'league': league})
    for match_1st in all_matches:
        for match_2nd in matches.find({'league': league}):
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


def fetch_finished_matches(league, season, limit=10, skip=0):
    matches_to_insert = []
    all_fixtures = fixtures.find(
        {'league.id': league, 'league.season': season}).sort({'fixture.id': 1}).skip(skip)
    try:
        for fixture in all_fixtures:
            match = matches.find_one({'fixture': fixture['fixture']['id']})
            if not match:
                print(fixture['league']['round'] + ' > ' + fixture['teams']
                      ['home']['name'] + ' : ' + fixture['teams']['away']['name'])
                finished_match = get_data(
                    f'/fixtures/statistics?fixture={fixture["fixture"]["id"]}')
                matches_to_insert.append(
                    {"homeTeam": finished_match["response"][0], "awayTeam": finished_match["response"][1], "fixture": fixture["fixture"]["id"], "league": fixture["league"]["id"], "season": fixture["league"]["season"], 'active': True})
            if len(matches_to_insert) >= limit:
                break
    finally:
        if len(matches_to_insert):
            matches.insert_many(matches_to_insert)


matches.update_many({}, {"$set": {'active': True}})

# update_features(135, 2024)
# fetch_finished_matches(135, 2024, 10, 0)
remove_old_matches(135)
