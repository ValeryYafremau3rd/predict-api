from services.db import fixtures, matches, leagues, teams
from datetime import date, timedelta, datetime

# 39 pl +
# 78 bl +
# 135 sa +
# 140 p +
# 61 l1
league_ids = [39, 78, 135, 140, 61]

# add fixtures


#check statisctics
def check_teams(league_id, season):
    print(f'\033[37m Teams:')
    unique_teams = set()
    updated_teams = set()
    for fixture in fixtures.find({'league.id': league_id, 'league.season': season}, {'teams.home.id': 1, 'teams.away.id': 1, 'teams.home.name': 1, 'teams.away.name': 1, 'teams.statistics': 1}):
        unique_teams.add(fixture['teams']['home']['id'])
        unique_teams.add(fixture['teams']['away']['id'])
    for unique_team in unique_teams:
        if teams.find_one({'league': league_id, 'season': season, 'team.id': unique_team}):
            updated_teams.add(unique_team)

    print(unique_teams)
    if len(unique_teams) == len(updated_teams):
        1#print('  All teams up to date')
    else:
        print(f'\033[31m  {len(unique_teams) - len(updated_teams)} team(s) not found')


def check_fixtures(league_id, season):
    print(f'\033[37m Fixtures:')
    yesterday = datetime.now().timestamp()
    if fixtures.find_one({'league.id': league_id, 'league.season': season-1}):
        if fixtures.find_one({'league.id': league_id, 'league.season': season-1, 'fixture.timestamp': {'$lt': yesterday}, 'fixture.status.long': {'$ne': 'Match Finished'}}):
            print(f'\033[31m  Season {season-1} fixtures are not updated')
        else:
            1#print(f'  Season {season} fixtures are up to date')
    else:
        print(f'\033[31m  Season {season-1} fixtures are not found')
    
    if fixtures.find_one({'league.id': league_id, 'league.season': season-1}):
        if fixtures.find_one({'league.id': league_id, 'league.season': season, 'fixture.timestamp': {'$lt': yesterday}, 'fixture.status.long': {'$ne': 'Match Finished'}}):
            print(f'\033[31m  Season {season} fixtures are not updated')
        else:
            1#print(f'  Season {season} fixtures are up to date')
    else:
        print(f'\033[31m  Season {season} fixtures are not found')


def check_matches(league_id, season):
    print(f'\033[37m Matches:')
    for fixture in fixtures.find({'league.id': league_id, 'league.season': season-1}):
        if not matches.find_one({'fixture': fixture['fixture']['id']}):
            print(f'\033[31m  Season {season-1} matches are not updated')
            break
    for fixture in fixtures.find({'league.id': league_id, 'league.season': season}):
        if not matches.find_one({'fixture': fixture['fixture']['id']}):
            print(f'\033[31m  Season {season} matches are not updated')
            break
    #dups
    found = False    
    all_matches = matches.find({'league': league_id, 'active': True})
    for match_1st in all_matches:
        for match_2nd in matches.find({'league': league_id, 'active': True}):
            if match_1st['_id'] != match_2nd['_id']:
                if match_2nd['fixture'] == match_1st['fixture'] or match_2nd['homeTeam']['team']['id'] == match_1st['homeTeam']['team']['id'] and match_2nd['awayTeam']['team']['id'] == match_1st['awayTeam']['team']['id']:
                    found = True
                    print( match_1st['_id'], match_2nd['_id'])
                    break
        if found:
            print(f'\033[31m  Duplicated matches')
            break

#relegated

season = 2025
for league in leagues.find({'league.id': {"$in": league_ids}}, {'league.name': 1, 'league.id': 1}):
    print(f"\033[37m\n{league['league']['name']}:")
    check_teams(league['league']['id'], season-1)
    check_fixtures(league['league']['id'], season)
    check_matches(league['league']['id'], season)
print(f"\033[37m\nDone.")


# all_fixtures = fixtures.find(
#        {'league.id': league, 'league.season': season}).sort({'fixture.id': 1})
