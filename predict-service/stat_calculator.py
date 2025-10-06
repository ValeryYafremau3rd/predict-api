import copy
import odd_builder as ob
from services.db import matches, fixtures
import statistics

# all_fixtures = fixtures.find()


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
            # all_fixtures
    # print(serializedStats['fixture'])
    return serializedStats


def collect_stats(matches, league_id, groups, season=2025):
    all_team_stats = {}
    pairs = []
    with matches.find({'league': league_id, 'active': True, "$or": [{"season": season}, {"season": season-1}]}).sort('time', 1) as mathcesCursor:
        for i, match in enumerate(mathcesCursor):

            fixture = fixtures.find_one({'fixture.id': match['fixture']})

            stats_home = serializeStats(match['homeTeam']['statistics'])
            stats_home['Half Time Goals'] = fixture["score"]["halftime"]["home"]
            stats_home['Goals'] = fixture["goals"]["home"]
            stats_home['is_home'] = True
            stats_away = serializeStats(match['awayTeam']['statistics'])
            stats_away['Half Time Goals'] = fixture["score"]["halftime"]["away"]
            stats_away['Goals'] = fixture["goals"]["away"]
            stats_away['is_home'] = False

            for stat in [x for x in stats_home if (not '%' in x and not 'is_home' in x)]:
                total = stats_home[stat] + stats_away[stat]
                if total != 0:
                    stats_home[stat + ' %'] = stats_home[stat] / total
                    stats_away[stat + ' %'] = stats_away[stat] / total
                else:
                    stats_home[stat + ' %'] = 0.5
                    stats_away[stat + ' %'] = 0.5

                goalRange = stats_home['Goals'] - stats_away['Goals']
                if goalRange != 0:
                    stats_home['Goal Range per ' +
                               stat] = (stats_away[stat] - stats_home[stat]) / goalRange
                    stats_away['Goal Range per ' +
                               stat] = (stats_home[stat] - stats_away[stat]) / goalRange
                else:
                    stats_home['Goal Range per ' + stat] = 0
                    stats_away['Goal Range per ' + stat] = 0

                if stats_home['Goals'] != 0:
                    stats_home[stat + ' / Goals'] = stats_home[stat] / \
                        stats_home['Goals']
                else:
                    stats_home[stat + ' / Goals'] = 0

                if stats_away['Goals'] != 0:
                    stats_away[stat + ' / Goals'] = stats_away[stat] / \
                        stats_away['Goals']
                else:
                    stats_away[stat + ' / Goals'] = 0

                if stats_home['Half Time Goals'] != 0:
                    stats_home[stat + ' / Half Time Goals'] = stats_home[stat] / \
                        stats_home['Half Time Goals']
                else:
                    stats_home[stat + ' / Half Time Goals'] = 0

                if stats_away['Half Time Goals'] != 0:
                    stats_away[stat + ' / Half Time Goals'] = stats_away[stat] / \
                        stats_away['Half Time Goals']
                else:
                    stats_away[stat + ' / Half Time Goals'] = 0

                if (stats_home['Goals'] - stats_home['Half Time Goals']) != 0:
                    stats_home[stat + ' / 2nd Half Goals'] = stats_home[stat] / \
                        (stats_home['Goals'] - stats_home['Half Time Goals'])
                else:
                    stats_home[stat + ' / 2nd Half Goals'] = 0

                if (stats_away['Goals'] - stats_away['Half Time Goals']) != 0:
                    stats_away[stat + ' / 2nd Half Goals'] = stats_away[stat] / \
                        (stats_away['Goals'] - stats_away['Half Time Goals'])
                else:
                    stats_away[stat + ' / 2nd Half Goals'] = 0

                htGoalRange = stats_home['Half Time Goals'] - \
                    stats_away['Half Time Goals']
                if htGoalRange != 0:
                    stats_home['1 half Goal Range per ' +
                               stat] = (stats_away[stat] - stats_home[stat]) / htGoalRange
                    stats_away['1 half Goal Range per ' +
                               stat] = (stats_home[stat] - stats_away[stat]) / htGoalRange
                else:
                    stats_home['1 half Goal Range per ' + stat] = 0
                    stats_away['1 half Goal Range per ' + stat] = 0

            stats_away['XG Difference'] = int(
                stats_away['expected_goals'] - stats_away['Goals'])
            stats_home['XG Difference'] = int(
                stats_home['expected_goals'] - stats_home['Goals'])

            stats_home['Win'] = int(stats_home['Goals'] > stats_away['Goals'])
            stats_home['Draw'] = int(
                stats_home['Goals'] == stats_away['Goals'])
            stats_home['Lose'] = int(stats_home['Goals'] < stats_away['Goals'])

            stats_away['Win'] = int(stats_home['Goals'] < stats_away['Goals'])
            stats_away['Draw'] = int(
                stats_home['Goals'] == stats_away['Goals'])
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
                    if not '%' in stat_name and not 'per' in stat_name and not '/' in stat_name and not 'Max' in stat_name and not 'Med' in stat_name:
                        stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                            stats_away[stat_name]
                        stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                            stats_home[stat_name]


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
                    if not '%' in stat_name and not 'per' in stat_name and not '/' in stat_name and not 'Max' in stat_name and not 'Med' in stat_name:
                        stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                            stats_away[stat_name]
                        stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                            stats_home[stat_name]


            # possible duplicate

            for stat in [x for x in stats_home if (not '%' in x and not 'is_home' in x and not ' Difference' in x and not ' Total' in x)]:
                total = stats_home[stat] + stats_away[stat]
                if total != 0:
                    stats_home[stat + ' %'] = stats_home[stat] / total
                    stats_away[stat + ' %'] = stats_away[stat] / total
                else:
                    stats_home[stat + ' %'] = 0.5
                    stats_away[stat + ' %'] = 0.5

                goalRange = stats_home['Goals'] - stats_away['Goals']
                if goalRange != 0:
                    stats_home['Goal Range per ' +
                               stat] = (stats_away[stat] - stats_home[stat]) / goalRange
                    stats_away['Goal Range per ' +
                               stat] = (stats_home[stat] - stats_away[stat]) / goalRange
                else:
                    stats_home['Goal Range per ' + stat] = 0
                    stats_away['Goal Range per ' + stat] = 0

                htGoalRange = stats_home['Half Time Goals'] - \
                    stats_away['Half Time Goals']
                if htGoalRange != 0:
                    stats_home['1 half Goal Range per ' +
                               stat] = (stats_away[stat] - stats_home[stat]) / htGoalRange
                    stats_away['1 half Goal Range per ' +
                               stat] = (stats_home[stat] - stats_away[stat]) / htGoalRange
                else:
                    stats_home['1 half Goal Range per ' + stat] = 0
                    stats_away['1 half Goal Range per ' + stat] = 0

                if stats_home['Goals'] != 0:
                    stats_home[stat + ' / Goals'] = stats_home[stat] / \
                        stats_home['Goals']
                else:
                    stats_home[stat + ' / Goals'] = 0

                if stats_away['Goals'] != 0:
                    stats_away[stat + ' / Goals'] = stats_away[stat] / \
                        stats_away['Goals']
                else:
                    stats_away[stat + ' / Goals'] = 0

                if stats_home['Half Time Goals'] != 0:
                    stats_home[stat + ' / Half Time Goals'] = stats_home[stat] / \
                        stats_home['Half Time Goals']
                else:
                    stats_home[stat + ' / Half Time Goals'] = 0

                if stats_away['Half Time Goals'] != 0:
                    stats_away[stat + ' / Half Time Goals'] = stats_away[stat] / \
                        stats_away['Half Time Goals']
                else:
                    stats_away[stat + ' / Half Time Goals'] = 0

            stats_away['XG Difference'] = int(
                stats_away['expected_goals'] - stats_away['Goals'])
            stats_home['XG Difference'] = int(
                stats_home['expected_goals'] - stats_home['Goals'])

            stats_home['Win'] = int(stats_home['Goals'] > stats_away['Goals'])
            stats_home['Draw'] = int(
                stats_home['Goals'] == stats_away['Goals'])
            stats_home['Lose'] = int(stats_home['Goals'] < stats_away['Goals'])

            stats_away['Win'] = int(stats_home['Goals'] < stats_away['Goals'])
            stats_away['Draw'] = int(
                stats_home['Goals'] == stats_away['Goals'])
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
                'season': match['season'],
                'fixture': match['fixture'],
                'awayTeamName': match['awayTeam']['team']['name'],
                'homeTeamName': match['homeTeam']['team']['name'],
                'awayTeamId': match['awayTeam']['team']['id'],
                'homeTeamId': match['homeTeam']['team']['id'],
                'homeTeamStats': stats_home,
                "awayTeamStats": stats_away
            })
    return (pairs, all_team_stats)

# sort by daTE


def xg_difference_shape(pairs, fixture, find_home_team, find_away_team, ht_match_range=3, at_match_range=3, season=False):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId'] or x['awayTeamId'] == pairs[last_match_index]['homeTeamId']][-ht_match_range:]
    home_team_shape = {}
    # check the length of pairs[:last_match_index + 1]
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['awayTeamId'] or x['awayTeamId'] == pairs[last_match_index]['awayTeamId']][-at_match_range:]
    away_team_shape = {}
    ht_prefix = 'This Season' if season else str(ht_match_range)
    at_prefix = 'This Season' if season else str(at_match_range)

    if find_home_team:
        for match in home_team_last_matches:
            if (match['homeTeamId'] == pairs[last_match_index]['homeTeamId']):
                home_team_shape.setdefault(
                    'Home Team XG Difference Shape ' + ht_prefix + ' Matches', 0.0)
                home_team_shape['Home Team XG Difference Shape ' + ht_prefix + ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + ht_prefix + ' Matches'] + \
                    match['Home Team expected_goals'] - \
                    match['Home Team Goals']
            else:
                home_team_shape.setdefault(
                    'Home Team XG Difference Shape ' + ht_prefix + ' Matches', 0.0)
                home_team_shape['Home Team XG Difference Shape ' + ht_prefix + ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + ht_prefix + ' Matches'] + \
                    match['Away Team expected_goals'] - \
                    match['Away Team Goals']

    if find_away_team:
        for match in away_team_last_matches:
            if (match['awayTeamId'] == pairs[last_match_index]['awayTeamId']):
                away_team_shape.setdefault(
                    'Away Team XG Difference Shape ' + at_prefix + ' Matches', 0.0)
                away_team_shape['Away Team XG Difference Shape ' + at_prefix + ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + at_prefix + ' Matches'] + \
                    match['Away Team expected_goals'] - \
                    match['Away Team Goals']
            else:
                away_team_shape.setdefault(
                    'Away Team XG Difference Shape ' + at_prefix + ' Matches', 0.0)
                away_team_shape['Away Team XG Difference Shape ' + at_prefix + ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + at_prefix + ' Matches'] + \
                    match['Home Team expected_goals'] - \
                    match['Home Team Goals']

    if 'Home Team XG Difference Shape ' + ht_prefix + ' Matches' in home_team_shape:
        home_team_shape['Home Team XG Difference Shape ' + ht_prefix +
                        ' Matches'] = home_team_shape['Home Team XG Difference Shape ' + ht_prefix + ' Matches'] / len(home_team_last_matches)
    if 'Away Team XG Difference Shape' in away_team_shape:
        away_team_shape['Away Team XG Difference Shape ' + at_prefix +
                        ' Matches'] = away_team_shape['Away Team XG Difference Shape ' + at_prefix + ' Matches'] / len(away_team_last_matches)
    return home_team_shape | away_team_shape

# sort by daTE


def home_away_shape(pairs, fixture, ht_match_range=3, at_match_range=3, season=False):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId']][-ht_match_range:]
    home_team_shape = {}
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['awayTeamId'] ==
                              pairs[last_match_index]['awayTeamId']][-at_match_range:]
    away_team_shape = {}
    ht_prefix = 'This Season' if season else str(ht_match_range)
    at_prefix = 'This Season' if season else str(at_match_range)
    for match in home_team_last_matches:
        for stat in match:
            if not stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                if not 'Avg' in stat and 'Home Team' in stat:
                    home_team_shape.setdefault(
                        stat + ' Home Shape ' + ht_prefix + ' Matches', 0.0)
                    home_team_shape[stat + ' Home Shape ' + ht_prefix + ' Matches'] = home_team_shape[stat + ' Home Shape ' + ht_prefix + ' Matches'] + \
                        match[stat] - match[stat.replace(
                            'Home Team', 'Away Team Avg Away Against')]

    for match in away_team_last_matches:
        for stat in match:
            if not stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                if not 'Avg' in stat and 'Away Team' in stat:
                    away_team_shape.setdefault(
                        stat + ' Away Shape ' + at_prefix + ' Matches', 0.0)
                    away_team_shape[stat + ' Away Shape ' + at_prefix + ' Matches'] = away_team_shape[stat + ' Away Shape ' + at_prefix + ' Matches'] + \
                        match[stat] - match[stat.replace(
                            'Away Team', 'Home Team Avg Home Against')]

    for stat in home_team_shape:
        home_team_shape[stat] = home_team_shape[stat] / \
            len(home_team_last_matches)
    for stat in away_team_shape:
        away_team_shape[stat] = away_team_shape[stat] / \
            len(away_team_last_matches)
    return home_team_shape | away_team_shape

# sort by daTE


def find_last_matches(pairs, fixture, find_home_team, find_away_team, ht_match_range=3, at_match_range=3, season=False):
    last_match_index = next((i for (i, x) in enumerate(
        pairs) if x["fixture"] == fixture), None)
    home_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['homeTeamId'] or x['awayTeamId'] == pairs[last_match_index]['homeTeamId']][-ht_match_range:]
    home_team_shape = {}
    away_team_last_matches = [x for x in pairs[:last_match_index + 1] if x['homeTeamId'] ==
                              pairs[last_match_index]['awayTeamId'] or x['awayTeamId'] == pairs[last_match_index]['awayTeamId']][-at_match_range:]
    away_team_shape = {}
    ht_prefix = 'This Season' if season else str(ht_match_range)
    at_prefix = 'This Season' if season else str(at_match_range)

    if find_home_team:
        for match in home_team_last_matches:
            if (match['homeTeamId'] == pairs[last_match_index]['homeTeamId']):
                for stat in match:
                    if not stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                        if not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Home Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Shape ' + ht_prefix + ' Matches', 0.0)
                            home_team_shape[stat + ' Shape ' + ht_prefix + ' Matches'] = home_team_shape[stat + ' Shape ' + ht_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
                        elif not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Away Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Against Shape ' + ht_prefix + ' Matches', 0.0)
                            home_team_shape[stat + ' Against Shape ' + ht_prefix + ' Matches'] = home_team_shape[stat + ' Against Shape ' + ht_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
            else:
                for stat in match:
                    reversed_stat = stat.replace('Away', 'Home')
                    if not reversed_stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                        if not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Home Team' in reversed_stat:
                            home_team_shape.setdefault(
                                stat + ' Shape ' + ht_prefix + ' Matches', 0.0)
                            home_team_shape[stat + ' Shape ' + ht_prefix + ' Matches'] = home_team_shape[stat + ' Shape ' + ht_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
                        elif not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Away Team' in stat:
                            home_team_shape.setdefault(
                                stat + ' Against Shape ' + ht_prefix + ' Matches', 0.0)
                            home_team_shape[stat + ' Against Shape ' + ht_prefix + ' Matches'] = home_team_shape[stat + ' Against Shape ' + ht_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]

    if find_away_team:
        for match in away_team_last_matches:
            if (match['awayTeamId'] == pairs[last_match_index]['awayTeamId']):
                for stat in match:
                    reversed_stat = stat.replace('Home', 'Away')
                    if not stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                        if not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Away Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Shape ' + at_prefix + ' Matches', 0.0)
                            away_team_shape[stat + ' Shape ' + at_prefix + ' Matches'] = away_team_shape[stat + ' Shape ' + at_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
                        elif not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Home Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Against Shape ' + at_prefix + ' Matches', 0.0)
                            away_team_shape[stat + ' Against Shape ' + at_prefix + ' Matches'] = away_team_shape[stat + ' Against Shape ' + at_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]
            else:
                for stat in match:
                    if not stat in ['fixture', 'season', 'awayTeamId', 'homeTeamId', 'awayTeamName', 'homeTeamName'] and not 'Total' in stat and not 'Shape' in stat and not 'Recent' in stat and not 'Team Stat' in stat:
                        if not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Away Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Shape ' + at_prefix + ' Matches', 0.0)
                            away_team_shape[stat + ' Shape ' + at_prefix + ' Matches'] = away_team_shape[stat + ' Shape ' + at_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Away Team', 'Home Team Avg Home Against')]
                        elif not 'Avg' in stat and not 'Med' in stat and not 'Min' in stat and not 'Max' in stat and 'Home Team' in stat:
                            away_team_shape.setdefault(
                                stat + ' Against Shape ' + at_prefix + ' Matches', 0.0)
                            away_team_shape[stat + ' Against Shape ' + at_prefix + ' Matches'] = away_team_shape[stat + ' Against Shape ' + at_prefix + ' Matches'] + \
                                match[stat] - match[stat.replace(
                                    'Home Team', 'Away Team Avg Away Against')]

    for stat in home_team_shape:
        home_team_shape[stat] = home_team_shape[stat] / \
            len(home_team_last_matches)
    for stat in away_team_shape:
        away_team_shape[stat] = away_team_shape[stat] / \
            len(away_team_last_matches)
    return home_team_shape | away_team_shape


def calculate_average_team_stats(team_stats):
    all_team_stats = {}
    all_team_home_stats = {}
    all_team_away_stats = {}

    avg_team_stats = {}
    avg_team_home_stats = {}
    avg_team_away_stats = {}

    med_team_stats = {}
    med_team_home_stats = {}
    med_team_away_stats = {}

    min_team_stats = {}
    min_team_home_stats = {}
    min_team_away_stats = {}

    max_team_stats = {}
    max_team_home_stats = {}
    max_team_away_stats = {}

    for teamName in team_stats:
        all_team_stats[teamName] = {}
        all_team_home_stats[teamName] = {}
        all_team_away_stats[teamName] = {}
        
        avg_team_stats[teamName] = {}
        avg_team_home_stats[teamName] = {}
        avg_team_away_stats[teamName] = {}
        
        med_team_stats[teamName] = {}
        med_team_home_stats[teamName] = {}
        med_team_away_stats[teamName] = {}
        
        min_team_stats[teamName] = {}
        min_team_home_stats[teamName] = {}
        min_team_away_stats[teamName] = {}
        
        max_team_stats[teamName] = {}
        max_team_home_stats[teamName] = {}
        max_team_away_stats[teamName] = {}


        for i, stats in enumerate(team_stats[teamName]):
            if stats['is_home'] == True:
                for statName in stats:
                    if not statName in all_team_home_stats[teamName]:
                        all_team_home_stats[teamName].setdefault(statName, [])
                    if isinstance(stats[statName], str) and ('%' in stats[statName]):
                        all_team_home_stats[teamName][statName].append(float(
                            stats[statName].replace('%', '')))
                    else:
                        if not stats[statName] is None:
                            all_team_home_stats[teamName][statName].append(float(
                                stats[statName]))
            else:
                for statName in stats:
                    if not statName in all_team_away_stats[teamName]:
                        all_team_away_stats[teamName].setdefault(statName, [])
                    if isinstance(stats[statName], str) and ('%' in stats[statName]):
                        all_team_away_stats[teamName][statName].append(float(
                            stats[statName].replace('%', '')))
                    else:
                        if not stats[statName] is None:
                            all_team_away_stats[teamName][statName].append(float(
                                stats[statName]))
            del stats['is_home']

            for statName in stats:
                if not statName in all_team_stats[teamName]:
                    all_team_stats[teamName].setdefault(statName, [])
                if isinstance(stats[statName], str) and ('%' in stats[statName]):
                    all_team_stats[teamName][statName].append(float(
                        stats[statName].replace('%', '')))
                else:
                    if not stats[statName] is None:
                        all_team_stats[teamName][statName].append(float(
                            stats[statName]))

    for teamName in all_team_stats:
        for statName in all_team_stats[teamName]:
            avg_team_stats[teamName][statName] = statistics.mean(
                all_team_stats[teamName][statName])
            med_team_stats[teamName][statName] = statistics.median(
                all_team_stats[teamName][statName])
            min_team_stats[teamName][statName] = min(
                all_team_stats[teamName][statName])
            max_team_stats[teamName][statName] = max(
                all_team_stats[teamName][statName])
    for teamName in all_team_home_stats:
        for statName in all_team_home_stats[teamName]:
            avg_team_home_stats[teamName][statName] = statistics.mean(
                all_team_home_stats[teamName][statName])
            med_team_home_stats[teamName][statName] = statistics.median(
                all_team_home_stats[teamName][statName])
            min_team_home_stats[teamName][statName] = min(
                all_team_home_stats[teamName][statName])
            max_team_home_stats[teamName][statName] = max(
                all_team_home_stats[teamName][statName])
    for teamName in all_team_away_stats:
        for statName in all_team_away_stats[teamName]:
            avg_team_away_stats[teamName][statName] = statistics.mean(
                all_team_away_stats[teamName][statName])
            med_team_away_stats[teamName][statName] = statistics.median(
                all_team_away_stats[teamName][statName])
            min_team_away_stats[teamName][statName] = min(
                all_team_away_stats[teamName][statName])
            max_team_away_stats[teamName][statName] = max(
                all_team_away_stats[teamName][statName])
    return (avg_team_stats, avg_team_home_stats, avg_team_away_stats,
            med_team_stats, med_team_home_stats, med_team_away_stats,
            min_team_stats, min_team_home_stats, min_team_away_stats,
            max_team_stats, max_team_home_stats, max_team_away_stats,)


def calculate_against_average_team_stats(all_team_stats):
    all_team_stats = {}
    for teamName in all_team_stats:
        all_team_stats[teamName] = {}
        for i, stats in enumerate(all_team_stats[teamName]):
            for statName in stats:
                if not statName in all_team_stats[teamName]:
                    all_team_stats[teamName].setdefault(statName, 0.0)
                if isinstance(stats[statName], str) and ('%' in stats[statName]):
                    all_team_stats[teamName][statName] += float(
                        stats[statName].replace('%', ''))
                else:
                    if not stats[statName] is None:
                        all_team_stats[teamName][statName] += float(
                            stats[statName])
                all_team_stats[teamName]["totalMatches"] = i + 1
    for teamName in all_team_stats:
        playedMatches = all_team_stats[teamName]["totalMatches"]
        for statName in all_team_stats[teamName]:
            all_team_stats[teamName][statName] = all_team_stats[teamName][statName] / playedMatches
        all_team_stats[teamName].pop('totalMatches', None)
    return all_team_stats


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

        for statName in pair['homeTeamMedStats']:
            pair['Home Team Med ' + statName] = pair['homeTeamMedStats'][statName]
        for statName in pair['awayTeamMedStats']:
            pair['Away Team Med ' + statName] = pair['awayTeamMedStats'][statName]
        for statName in pair['homeTeamMedHomeStats']:
            pair['Home Team Med Home ' +
                 statName] = pair['homeTeamMedHomeStats'][statName]
        for statName in pair['awayTeamMedAwayStats']:
            pair['Away Team Med Away ' +
                 statName] = pair['awayTeamMedAwayStats'][statName]
#
#        for statName in pair['homeTeamMinStats']:
#            pair['Home Team Min ' + statName] = pair['homeTeamMinStats'][statName]
#        for statName in pair['awayTeamMinStats']:
#            pair['Away Team Min ' + statName] = pair['awayTeamMinStats'][statName]
#        for statName in pair['homeTeamMinHomeStats']:
#            pair['Home Team Min Home ' +
#                 statName] = pair['homeTeamMinHomeStats'][statName]
#        for statName in pair['awayTeamMinAwayStats']:
#            pair['Away Team Min Away ' +
#                 statName] = pair['awayTeamMinAwayStats'][statName]
#
#        for statName in pair['homeTeamMaxStats']:
#            pair['Home Team Max ' + statName] = pair['homeTeamMaxStats'][statName]
#        for statName in pair['awayTeamMaxStats']:
#            pair['Away Team Max ' + statName] = pair['awayTeamMaxStats'][statName]
#        for statName in pair['homeTeamMaxHomeStats']:
#            pair['Home Team Max Home ' +
#                 statName] = pair['homeTeamMaxHomeStats'][statName]
#        for statName in pair['awayTeamMaxAwayStats']:
#            pair['Away Team Max Away ' +
#                 statName] = pair['awayTeamMaxAwayStats'][statName]

        pair.pop('homeTeamStats', None)
        pair.pop('awayTeamStats', None)

        pair.pop('homeTeamAvgStats', None)
        pair.pop('awayTeamAvgStats', None)
        pair.pop('homeTeamAvgHomeStats', None)
        pair.pop('awayTeamAvgAwayStats', None)

        pair.pop('homeTeamMedStats', None)
        pair.pop('awayTeamMedStats', None)
        pair.pop('homeTeamMedHomeStats', None)
        pair.pop('awayTeamMedAwayStats', None)

        pair.pop('homeTeamMinStats', None)
        pair.pop('awayTeamMinStats', None)
        pair.pop('homeTeamMinHomeStats', None)
        pair.pop('awayTeamMinAwayStats', None)

        pair.pop('homeTeamMaxStats', None)
        pair.pop('awayTeamMaxStats', None)
        pair.pop('homeTeamMaxHomeStats', None)
        pair.pop('awayTeamMaxAwayStats', None)

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
                if not '%' in stat_name and not 'per' in stat_name and not '/' in stat_name and not 'Max' in stat_name and not 'Med' in stat_name:
                    stats_home[stat_name + ' Total'] = stats_home[stat_name] + \
                        stats_away[stat_name]
                    stats_away[stat_name + ' Total'] = stats_away[stat_name] + \
                        stats_home[stat_name]

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
        if not 'Name' in statName and not 'Avg' in statName and not 'Med' in statName and not 'Min' in statName and not 'Max' in statName:
            res.append(statName)
    return res
