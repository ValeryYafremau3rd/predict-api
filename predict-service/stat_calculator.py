import copy
import pymongo
import odd_builder as ob
smallest = 4.9406564584124654e-100
def avoid_zero(val):
    return val if val > 0 else smallest

myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
matches = myclient["statistics"]["matches"]
fixtures = myclient["statistics"]["fixtures"]
relations = myclient["statistics"]["relations"]
all_fixtures = fixtures.find()


def serializeStats(stats):
    serializedStats = {}
    for stat in stats:
        if isinstance(stat["value"], str):
            serializedStats[stat["type"]] = float(
                stat["value"].replace('%', ''))
        elif not stat["value"]:
            serializedStats[stat["type"]] = 0.0
        else:
            serializedStats[stat["type"]] = stat["value"]
            all_fixtures
    # print(serializedStats['fixture'])
    return serializedStats


def collect_stats(matches, league_id, groups, season=2024):
    all_team_stats = {}
    pairs = []

    for i, match in enumerate(matches.find({'league': league_id, 'active': True})):
        # optimize
        if not (matches.find_one({'league': league_id, 'season': season, 'homeTeam.team.id': match['homeTeam']['team']['id']}) and
                matches.find_one({'league': league_id, 'season': season, 'homeTeam.team.id': match['awayTeam']['team']['id']}) and
                matches.find_one({'league': league_id, 'season': season-1, 'homeTeam.team.id': match['homeTeam']['team']['id']}) and
                matches.find_one({'league': league_id, 'season': season-1, 'homeTeam.team.id': match['awayTeam']['team']['id']}) and
                matches.find_one({'league': league_id, 'season': season-2, 'homeTeam.team.id': match['homeTeam']['team']['id']}) and
                matches.find_one({'league': league_id, 'season': season-2, 'homeTeam.team.id': match['awayTeam']['team']['id']})):
            continue

        fixture = fixtures.find_one({'fixture.id': match['fixture']})

        stats_home = serializeStats(match['homeTeam']['statistics'])
        stats_home['Half Time Goals'] = fixture["score"]["halftime"]["home"]
        stats_home['Goals'] = fixture["goals"]["home"]
        stats_home['is_home'] = True
        stats_away = serializeStats(match['awayTeam']['statistics'])
        stats_away['Half Time Goals'] = fixture["score"]["halftime"]["away"]
        stats_away['Goals'] = fixture["goals"]["away"]
        stats_away['is_home'] = False

        stats_away['XG Difference'] = int(
            stats_away['expected_goals'] - stats_away['Goals'])
        stats_home['XG Difference'] = int(
            stats_home['expected_goals'] - stats_home['Goals'])

        stats_home['Win'] = int(stats_home['Goals'] > stats_away['Goals'])
        stats_home['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_home['Lose'] = int(stats_home['Goals'] < stats_away['Goals'])

        stats_away['Win'] = int(stats_home['Goals'] < stats_away['Goals'])
        stats_away['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_away['Lose'] = int(stats_home['Goals'] > stats_away['Goals'])

        stats_home['Half Time Win'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])
        stats_home['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_home['Half Time Lose'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])

        stats_away['Half Time Win'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])
        stats_away['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_away['Half Time Lose'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])

        stats_home['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])

        stats_away['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])

        flatten_home_away_stats = {}
        for stat in stats_home:
            flatten_home_away_stats[f'Home Team {stat}'] = stats_home[stat]
        for stat in stats_away:
            flatten_home_away_stats[f'Away Team {stat}'] = stats_away[stat]
        for group in groups:
            calculated_groups = ob.extract_odds_from_group(
                group, flatten_home_away_stats)
            for odd in calculated_groups:
                stats_home[odd + ' Odd'] = calculated_groups[odd]
                stats_away[odd + ' Odd'] = calculated_groups[odd]

        serializedHomeTeamStats = copy.deepcopy(stats_home)
        serializedAwayTeamStats = copy.deepcopy(stats_away)

        for stat_name in {**serializedHomeTeamStats}:
            if stat_name != 'is_home':
                serializedHomeTeamStats[stat_name + ' Difference'] = serializedHomeTeamStats[stat_name] - \
                    serializedAwayTeamStats[stat_name]
                serializedAwayTeamStats[stat_name + ' Difference'] = serializedAwayTeamStats[stat_name] - \
                    serializedHomeTeamStats[stat_name]

        for stat_name in {**stats_home}:
            if stat_name != 'is_home':
                stats_home[stat_name + ' Difference'] = stats_home[stat_name] - \
                    stats_away[stat_name]
                stats_away[stat_name + ' Difference'] = stats_away[stat_name] - \
                    stats_home[stat_name]
                stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                    stats_away[stat_name]
                stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                    stats_home[stat_name]
            #for stat_name_2nd in {**stats_home}:
            #    if stat_name != stat_name_2nd:
            #        stats_home['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_home['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_home['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])

        for key in stats_home:
            serializedAwayTeamStats['Against ' + key] = stats_home[key]
        for key in stats_away:
            serializedHomeTeamStats['Against ' + key] = stats_away[key]

        all_team_stats.setdefault(match['homeTeam']['team']['id'], [])
        all_team_stats[match['homeTeam']['team']
                       ['id']].append(serializedHomeTeamStats)

        all_team_stats.setdefault(match['awayTeam']['team']['id'], [])
        all_team_stats[match['awayTeam']['team']
                       ['id']].append(serializedAwayTeamStats)

        stats_home = serializeStats(match['homeTeam']['statistics'])
        stats_home['Half Time Goals'] = fixture["score"]["halftime"]["home"]
        stats_home['Goals'] = fixture["goals"]["home"]
        stats_away = serializeStats(match['awayTeam']['statistics'])
        stats_away['Half Time Goals'] = fixture["score"]["halftime"]["away"]
        stats_away['Goals'] = fixture["goals"]["away"]

        for stat_name in {**stats_home}:
            if stat_name != 'is_home':
                stats_home[stat_name + ' Difference'] = stats_home[stat_name] - \
                    stats_away[stat_name]
                stats_away[stat_name + ' Difference'] = stats_away[stat_name] - \
                    stats_home[stat_name]
                stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                    stats_away[stat_name]
                stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                    stats_home[stat_name]
            #for stat_name_2nd in {**stats_home}:
            #    if stat_name != stat_name_2nd:
            #        stats_home['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_home['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_home['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])

        # possible duplicate
        stats_away['XG Difference'] = int(
            stats_away['expected_goals'] - stats_away['Goals'])
        stats_home['XG Difference'] = int(
            stats_home['expected_goals'] - stats_home['Goals'])

        stats_home['Win'] = int(stats_home['Goals'] > stats_away['Goals'])
        stats_home['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_home['Lose'] = int(stats_home['Goals'] < stats_away['Goals'])

        stats_away['Win'] = int(stats_home['Goals'] < stats_away['Goals'])
        stats_away['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_away['Lose'] = int(stats_home['Goals'] > stats_away['Goals'])

        stats_home['Half Time Win'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])
        stats_home['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_home['Half Time Lose'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])

        stats_away['Half Time Win'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])
        stats_away['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_away['Half Time Lose'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])

        stats_home['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])

        stats_away['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])

        flatten_home_away_stats = {}
        for stat in stats_home:
            flatten_home_away_stats[f'Home Team {stat}'] = stats_home[stat]
        for stat in stats_away:
            flatten_home_away_stats[f'Away Team {stat}'] = stats_away[stat]
        for group in groups:
            calculated_groups = ob.extract_odds_from_group(
                group, flatten_home_away_stats)
            for odd in calculated_groups:
                stats_home[odd + ' Odd'] = calculated_groups[odd]
                stats_away[odd + ' Odd'] = calculated_groups[odd]

        pairs.append({
            'fixture': match['fixture'],
            'awayTeamName': match['awayTeam']['team']['name'],
            'homeTeamName': match['homeTeam']['team']['name'],
            'awayTeamId': match['awayTeam']['team']['id'],
            'homeTeamId': match['homeTeam']['team']['id'],
            'homeTeamStats': stats_home,
            "awayTeamStats": stats_away
        })
    return (pairs, all_team_stats)


def xg_difference_shape(pairs, fixture, find_home_team, find_away_team, match_range=3):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId'] or x['awayTeamId'] == pairs[last_match_index]['homeTeamId']][-match_range-1:]
    home_team_shape = {}
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['awayTeamId'] or x['awayTeamId'] == pairs[last_match_index]['awayTeamId']][-match_range-1:]
    away_team_shape = {}

    if find_home_team:
        for match in home_team_last_matches:
            if (match['homeTeamId'] == pairs[last_match_index]['homeTeamId']):
                home_team_shape.setdefault(
                    'Home Team XG Difference Shape ' + str(match_range) + ' Matches', 0.0)
                home_team_shape['Home Team XG Difference Shape ' + str(match_range) + ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + str(match_range) + ' Matches'] + \
                    match['Home Team expected_goals'] - \
                    match['Home Team Goals']
            else:
                home_team_shape.setdefault(
                    'Home Team XG Difference Shape ' + str(match_range) + ' Matches', 0.0)
                home_team_shape['Home Team XG Difference Shape ' + str(match_range) + ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + str(match_range) + ' Matches'] + \
                    match['Away Team expected_goals'] - \
                    match['Away Team Goals']

    if find_away_team:
        for match in away_team_last_matches:
            if (match['awayTeamId'] == pairs[last_match_index]['awayTeamId']):
                away_team_shape.setdefault(
                    'Away Team XG Difference Shape ' + str(match_range) + ' Matches', 0.0)
                away_team_shape['Away Team XG Difference Shape ' + str(match_range) + ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + str(match_range) + ' Matches'] + \
                    match['Away Team expected_goals'] - \
                    match['Away Team Goals']
            else:
                away_team_shape.setdefault(
                    'Away Team XG Difference Shape ' + str(match_range) + ' Matches', 0.0)
                away_team_shape['Away Team XG Difference Shape ' + str(match_range) + ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + str(match_range) + ' Matches'] + \
                    match['Home Team expected_goals'] - \
                    match['Home Team Goals']

    if 'Home Team XG Difference Shape ' + str(match_range) + ' Matches' in home_team_shape:
        home_team_shape['Home Team XG Difference Shape ' + str(match_range) + ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + str(
            match_range) + ' Matches'] / len(home_team_last_matches)
    if 'Away Team XG Difference Shape' in away_team_shape:
        away_team_shape['Away Team XG Difference Shape ' + str(match_range) + ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + str(
            match_range) + ' Matches'] / len(away_team_last_matches)
    return home_team_shape | away_team_shape

def home_away_shape(pairs, fixture, match_range=3):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId']][-match_range-1:]
    home_team_shape = {}
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['awayTeamId'] ==
                              pairs[last_match_index]['awayTeamId']][-match_range-1:]
    away_team_shape = {}
    for match in home_team_last_matches:
        for stat in match:
            if not stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                if not 'Avg' in stat and 'Home Team' in stat:
                    home_team_shape.setdefault(
                        stat + ' Home Shape ' + str(match_range) + ' Matches', 0.0)
                    home_team_shape[stat + ' Home Shape ' + str(match_range) + ' Matches'] = home_team_shape[stat + ' Home Shape ' + str(match_range) + ' Matches'] + \
                        match[stat] - match[stat.replace(
                            'Home Team', 'Away Team Avg Away Against')]

    for match in away_team_last_matches:
        for stat in match:
            if not stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                if not 'Avg' in stat and 'Awway Team' in stat:
                    away_team_shape.setdefault(
                        stat + ' Away Shape ' + str(match_range) + ' Matches', 0.0)
                    away_team_shape[stat + ' Away Shape ' + str(match_range) + ' Matches'] = away_team_shape[stat + ' Away Shape ' + str(match_range) + ' Matches'] + \
                        match[stat] - match[stat.replace(
                            'Home Team', 'Home Team Avg Home Against')]
                            
    for stat in home_team_shape:
        home_team_shape[stat] = home_team_shape[stat] / \
            len(home_team_last_matches)
    for stat in away_team_shape:
        away_team_shape[stat] = away_team_shape[stat] / \
            len(away_team_last_matches)
    return home_team_shape | away_team_shape

def find_last_matches(pairs, fixture, find_home_team, find_away_team, match_range=3):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId'] or x['awayTeamId'] == pairs[last_match_index]['homeTeamId']][-match_range-1:]
    home_team_shape = {}
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['awayTeamId'] or x['awayTeamId'] == pairs[last_match_index]['awayTeamId']][-match_range-1:]
    away_team_shape = {}

    if find_home_team:
        for match in home_team_last_matches:
            if (match['homeTeamId'] == pairs[last_match_index]['homeTeamId']):
                for stat in match:
                    if not stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                        if not 'Avg' in stat and 'Home Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Shape ' + str(match_range) + ' Matches', 0.0)
                            home_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] = home_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
                        elif not 'Avg' in stat and 'Away Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Against Shape ' + str(match_range) + ' Matches', 0.0)
                            home_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] = home_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
            else:
                for stat in match:
                    reversed_stat = stat.replace('Away', 'Home')
                    if not reversed_stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                        if not 'Avg' in stat and 'Home Team' in reversed_stat:
                            home_team_shape.setdefault(
                                stat + ' Shape ' + str(match_range) + ' Matches', 0.0)
                            home_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] = home_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
                        elif not 'Avg' in stat and 'Away Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Against Shape ' + str(match_range) + ' Matches', 0.0)
                            home_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] = home_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]

    if find_away_team:
        for match in away_team_last_matches:
            if (match['awayTeamId'] == pairs[last_match_index]['awayTeamId']):
                for stat in match:
                    reversed_stat = stat.replace('Home', 'Away')
                    if not stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                        if not 'Avg' in stat and 'Away Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Shape ' + str(match_range) + ' Matches', 0.0)
                            away_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] = away_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
                        elif not 'Avg' in stat and 'Home Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Against Shape ' + str(match_range) + ' Matches', 0.0)
                            away_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] = away_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
            else:
                for stat in match:
                    if not stat in ['fixture', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Shape' in stat and not 'Recent' in stat:
                        if not 'Avg' in stat and 'Away Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Shape ' + str(match_range) + ' Matches', 0.0)
                            away_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] = away_team_shape[stat + ' Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
                        elif not 'Avg' in stat and 'Home Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Against Shape ' + str(match_range) + ' Matches', 0.0)
                            away_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] = away_team_shape[stat + ' Against Shape ' + str(match_range) + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]

    for stat in home_team_shape:
        home_team_shape[stat] = home_team_shape[stat] / \
            len(home_team_last_matches)
    for stat in away_team_shape:
        away_team_shape[stat] = away_team_shape[stat] / \
            len(away_team_last_matches)
    return home_team_shape | away_team_shape


def calculate_average_team_stats(all_team_stats):
    avg_team_stats = {}
    avg_team_home_stats = {}
    avg_team_away_stats = {}

    for teamName in all_team_stats:
        avg_team_stats[teamName] = {}
        avg_team_home_stats[teamName] = {}
        avg_team_home_stats[teamName]["totalMatches"] = 0
        avg_team_away_stats[teamName] = {}
        avg_team_away_stats[teamName]["totalMatches"] = 0

        for i, stats in enumerate(all_team_stats[teamName]):
            if stats['is_home'] == True:
                avg_team_home_stats[teamName]["totalMatches"] = avg_team_home_stats[teamName]["totalMatches"] + 1
                for statName in stats:
                    if not statName in avg_team_home_stats[teamName]:
                        avg_team_home_stats[teamName].setdefault(statName, 0.0)
                    if isinstance(stats[statName], str) and ('%' in stats[statName]):
                        avg_team_home_stats[teamName][statName] += float(
                            stats[statName].replace('%', ''))
                    else:
                        if not stats[statName] is None:
                            avg_team_home_stats[teamName][statName] += float(
                                stats[statName])
            else:
                avg_team_away_stats[teamName]["totalMatches"] = avg_team_away_stats[teamName]["totalMatches"] + 1
                for statName in stats:
                    if not statName in avg_team_away_stats[teamName]:
                        avg_team_away_stats[teamName].setdefault(statName, 0.0)
                    if isinstance(stats[statName], str) and ('%' in stats[statName]):
                        avg_team_away_stats[teamName][statName] += float(
                            stats[statName].replace('%', ''))
                    else:
                        if not stats[statName] is None:
                            avg_team_away_stats[teamName][statName] += float(
                                stats[statName])
            del stats['is_home']

            for statName in stats:
                if not statName in avg_team_stats[teamName]:
                    avg_team_stats[teamName].setdefault(statName, 0.0)
                if isinstance(stats[statName], str) and ('%' in stats[statName]):
                    avg_team_stats[teamName][statName] += float(
                        stats[statName].replace('%', ''))
                else:
                    if not stats[statName] is None:
                        avg_team_stats[teamName][statName] += float(
                            stats[statName])
                avg_team_stats[teamName]["totalMatches"] = i + 1

    for teamName in avg_team_stats:
        playedMatches = avg_team_stats[teamName]["totalMatches"]
        for statName in avg_team_stats[teamName]:
            avg_team_stats[teamName][statName] = avg_team_stats[teamName][statName] / playedMatches
        avg_team_stats[teamName].pop('totalMatches', None)
    for teamName in avg_team_home_stats:
        playedMatches = avg_team_home_stats[teamName]["totalMatches"]
        for statName in avg_team_home_stats[teamName]:
            avg_team_home_stats[teamName][statName] = avg_team_home_stats[teamName][statName] / playedMatches
        avg_team_stats[teamName].pop('totalMatches', None)
    for teamName in avg_team_away_stats:
        playedMatches = avg_team_away_stats[teamName]["totalMatches"]
        for statName in avg_team_away_stats[teamName]:
            avg_team_away_stats[teamName][statName] = avg_team_away_stats[teamName][statName] / playedMatches
        avg_team_away_stats[teamName].pop('totalMatches', None)
    return (avg_team_stats, avg_team_home_stats, avg_team_away_stats)


def calculate_against_average_team_stats(all_team_stats):
    avg_team_stats = {}
    for teamName in all_team_stats:
        avg_team_stats[teamName] = {}
        for i, stats in enumerate(all_team_stats[teamName]):
            for statName in stats:
                if not statName in avg_team_stats[teamName]:
                    avg_team_stats[teamName].setdefault(statName, 0.0)
                if isinstance(stats[statName], str) and ('%' in stats[statName]):
                    avg_team_stats[teamName][statName] += float(
                        stats[statName].replace('%', ''))
                else:
                    if not stats[statName] is None:
                        avg_team_stats[teamName][statName] += float(
                            stats[statName])
                avg_team_stats[teamName]["totalMatches"] = i + 1
    for teamName in avg_team_stats:
        playedMatches = avg_team_stats[teamName]["totalMatches"]
        for statName in avg_team_stats[teamName]:
            avg_team_stats[teamName][statName] = avg_team_stats[teamName][statName] / playedMatches
        avg_team_stats[teamName].pop('totalMatches', None)
    return avg_team_stats


def flatten_stats(pairs):
    flatten = pairs[:]
    for pair in flatten:
        for statName in pair['homeTeamStats']:
            pair['Home Team ' + statName] = pair['homeTeamStats'][statName]
        for statName in pair['awayTeamStats']:
            pair['Away Team ' + statName] = pair['awayTeamStats'][statName]
        for statName in pair['homeTeamAvgStats']:
            pair['Home Team Avg ' + statName] = pair['homeTeamAvgStats'][statName]
        for statName in pair['awayTeamAvgStats']:
            pair['Away Team Avg ' + statName] = pair['awayTeamAvgStats'][statName]
        for statName in pair['homeTeamAvgHomeStats']:
            pair['Home Team Avg Home ' +
                 statName] = pair['homeTeamAvgHomeStats'][statName]
        for statName in pair['awayTeamAvgAwayStats']:
            pair['Away Team Avg Away ' +
                 statName] = pair['awayTeamAvgAwayStats'][statName]
        pair.pop('homeTeamStats', None)
        pair.pop('awayTeamStats', None)
        pair.pop('homeTeamAvgStats', None)
        pair.pop('awayTeamAvgStats', None)
        pair.pop('homeTeamAvgHomeStats', None)
        pair.pop('awayTeamAvgAwayStats', None)
    return flatten


def add_groups(pairs, groups):
    calculated_groups = []
    for pair in pairs:
        new_pair = {**pair}
        for group in groups:
            new_pair = {**new_pair, **ob.extract_odds_from_group(group, pair)}
        calculated_groups.append(new_pair)
    return calculated_groups


def win_draw_lose(pairs):
    wdl_pairs = []
    for i, pair in enumerate(pairs):
        new_pair = {**pair}
        wdl_pair = {'win': 0, 'draw': 0, 'lose': 0}
        dc_pair = {'dc_ft_wx': 0, 'dc_ft_wl': 0, 'dc_ft_xl': 0}
        goals_distribution = {'1h_more_goals': 0,
                              'both_halfs_same_goals': 0, '2h_more_goals': 0}
        h_wdl_pair = {'h_win': 0, 'h_draw': 0, 'h_lose': 0}
        second_h_wdl_pair = {'2h_win': 0, '2h_draw': 0, '2h_lose': 0}
        ht_goals_distribution = {'ht_first_half': 0,
                                 'ht_equal': 0, 'ht_second_half': 0}
        at_goals_distribution = {'at_first_half': 0,
                                 'at_equal': 0, 'at_second_half': 0}

        home_team_second_half_goals = new_pair['Home Team Goals'] > new_pair['Home Team Half Time Goals']
        away_team_second_half_goals = new_pair['Away Team Goals'] > new_pair['Away Team Half Time Goals']

        # both teams to score
        if new_pair['Home Team Goals'] > 0 and new_pair['Home Team Goals'] > 0:
            new_pair['both_teams_to_score'] = 1
        else:
            new_pair['both_teams_to_score'] = 0

        # each team to score 2+
        if new_pair['Home Team Goals'] >= 2 and new_pair['Home Team Goals'] >= 2:
            new_pair['both_teams_to_score_2+'] = 1
        else:
            new_pair['both_teams_to_score_2+'] = 0

        # home team either half winner
        if home_team_second_half_goals > away_team_second_half_goals or new_pair['Home Team Half Time Goals'] > new_pair['Away Team Half Time Goals']:
            new_pair['home_team_either_half_win'] = 1
        else:
            new_pair['home_team_either_half_win'] = 0

        # away team either half winner
        if home_team_second_half_goals < away_team_second_half_goals or new_pair['Home Team Half Time Goals'] < new_pair['Away Team Half Time Goals']:
            new_pair['away_team_either_half_win'] = 1
        else:
            new_pair['away_team_either_half_win'] = 0

        # 2nd half winer
        if home_team_second_half_goals > away_team_second_half_goals:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_win': 1}
        elif home_team_second_half_goals < away_team_second_half_goals:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_lose': 1}
        else:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_draw': 1}

        # scores comparison by halves
        if new_pair['Home Team Half Time Goals'] + new_pair['Away Team Half Time Goals'] > home_team_second_half_goals + away_team_second_half_goals:
            new_pair = {**goals_distribution, **new_pair, '1h_more_goals': 1}
        elif new_pair['Home Team Half Time Goals'] + new_pair['Away Team Half Time Goals'] < home_team_second_half_goals + away_team_second_half_goals:
            new_pair = {**goals_distribution, **new_pair, '2h_more_goals': 1}
        else:
            new_pair = {**goals_distribution, **
                        new_pair, 'both_halfs_same_goals': 1}

        # 2nd half winer
        if home_team_second_half_goals > away_team_second_half_goals:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_win': 1}
        elif home_team_second_half_goals < away_team_second_half_goals:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_lose': 1}
        else:
            new_pair = {**second_h_wdl_pair, **new_pair, '2h_draw': 1}

        # winner
        if new_pair['Home Team Goals'] > new_pair['Away Team Goals']:
            new_pair = {**wdl_pair, **new_pair, 'win': 1}
        elif pair['Home Team Goals'] < new_pair['Away Team Goals']:
            new_pair = {**wdl_pair, **new_pair, 'lose': 1}
        else:
            new_pair = {**wdl_pair, **new_pair, 'draw': 1}

        if new_pair['Home Team Half Time Goals'] > new_pair['Away Team Half Time Goals']:
            new_pair = {**new_pair, **h_wdl_pair, 'h_win': 1}
        elif pair['Home Team Half Time Goals'] < new_pair['Away Team Half Time Goals']:
            new_pair = {**new_pair, **h_wdl_pair, 'h_lose': 1}
        else:
            new_pair = {**new_pair, **h_wdl_pair, 'h_draw': 1}

        if new_pair['Home Team Goals'] - new_pair['Home Team Half Time Goals'] > new_pair['Home Team Half Time Goals']:
            new_pair = {**new_pair, **
                        ht_goals_distribution, 'ht_second_half': 1}
        elif new_pair['Home Team Goals'] - new_pair['Home Team Half Time Goals'] < new_pair['Home Team Half Time Goals']:
            new_pair = {**new_pair, **
                        ht_goals_distribution, 'ht_first_half': 1}
        else:
            new_pair = {**new_pair, **ht_goals_distribution, 'ht_equal': 1}

        if new_pair['Away Team Goals'] - new_pair['Away Team Half Time Goals'] > new_pair['Away Team Half Time Goals']:
            new_pair = {**new_pair, **
                        at_goals_distribution, 'at_second_half': 1}
        elif new_pair['Away Team Goals'] - new_pair['Away Team Half Time Goals'] < new_pair['Away Team Half Time Goals']:
            new_pair = {**new_pair, **
                        at_goals_distribution, 'at_first_half': 1}
        else:
            new_pair = {**new_pair, **at_goals_distribution, 'at_equal': 1}

        wdl_pairs.append(new_pair)
    return wdl_pairs


def recent_encounter(pairs):
    pairs_with_recent_encounters = []
    for pair in pairs:
        new_pair = {**pair}
        match = matches.find_one(
            {'homeTeam.team.name': pair['homeTeamName'], 'awayTeam.team.name': pair['awayTeamName'], 'active': False})

        fixture = fixtures.find_one({'fixture.id': match['fixture']})
        stats_home = serializeStats(match['homeTeam']['statistics'])
        stats_home['Half Time Goals'] = fixture["score"]["halftime"]["home"]
        stats_home['Goals'] = fixture["goals"]["home"]
        stats_home['is_home'] = True
        stats_away = serializeStats(match['awayTeam']['statistics'])
        stats_away['Half Time Goals'] = fixture["score"]["halftime"]["away"]
        stats_away['Goals'] = fixture["goals"]["away"]
        stats_away['is_home'] = False

        for stat_name in {**stats_home}:
            if stat_name != 'is_home':
                stats_home[stat_name + ' Difference'] = stats_home[stat_name] - \
                    stats_away[stat_name]
                stats_away[stat_name + ' Difference'] = stats_away[stat_name] - \
                    stats_home[stat_name]
                stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                    stats_away[stat_name]
                stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                    stats_home[stat_name]
            #for stat_name_2nd in {**stats_home}:
            #    if stat_name != stat_name_2nd:
            #        stats_home['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_home['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])
            #        #stats_away['Self '+ stat_name + ' / Opponent ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_home['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_away[stat_name] / avoid_zero(stats_home[stat_name_2nd])
            #        #stats_away['Opponent '+ stat_name + ' / Self ' + stat_name_2nd] = stats_home[stat_name] / avoid_zero(stats_away[stat_name_2nd])


# duplicates for home and away
        stats_home['Win'] = int(stats_home['Goals'] > stats_away['Goals'])
        stats_home['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_home['Lose'] = int(stats_home['Goals'] < stats_away['Goals'])

        stats_away['Win'] = int(stats_home['Goals'] < stats_away['Goals'])
        stats_away['Draw'] = int(stats_home['Goals'] == stats_away['Goals'])
        stats_away['Lose'] = int(stats_home['Goals'] > stats_away['Goals'])

        stats_home['Half Time Win'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])
        stats_home['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_home['Half Time Lose'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])

        stats_away['Half Time Win'] = int(
            stats_home['Half Time Goals'] < stats_away['Half Time Goals'])
        stats_away['Half Time Draw'] = int(
            stats_home['Half Time Goals'] == stats_away['Half Time Goals'])
        stats_away['Half Time Lose'] = int(
            stats_home['Half Time Goals'] > stats_away['Half Time Goals'])

        stats_home['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_home['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])

        stats_away['2nd Half Win'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] < stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Draw'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] == stats_away['Goals'] - stats_away['Half Time Goals'])
        stats_away['2nd Half Lose'] = int(
            stats_home['Goals'] - stats_home['Half Time Goals'] > stats_away['Goals'] - stats_away['Half Time Goals'])

        #for stat in stats_home:
        #    new_pair[f'Recent Encounter Home Team {stat}'] = stats_home[stat]
        #    new_pair[f'Recent Encounter Away Team {stat}'] = stats_away[stat]
        pairs_with_recent_encounters.append(new_pair)
    return pairs_with_recent_encounters


def getStatsByTeamName(teamStats, isHomeTeam=True):
    res = [[], []]
    for statName in teamStats:
        res[0].append(
            'Home Team ' + statName if isHomeTeam else 'Away Team ' + statName)
        res[1].append(teamStats[statName])
    return res


def getResultStatsNames(pairs):
    res = []
    for statName in pairs[0]:
        if not 'Name' in statName and not 'Avg' in statName:
            res.append(statName)
    return res
