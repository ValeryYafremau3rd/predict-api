import threading
import time
import traceback
import redis
import os
import json
import stat_calculator as sc
import predictor as pd
import pandas
from sklearn.preprocessing import StandardScaler
from itertools import combinations
import statistics
from bson.objectid import ObjectId
from services.db import matches, fixtures, queue, predicted, teams
from services.numbers import calibrate_chanses, min_value, max_value, avoid_zero_value, expand_odds

print_deps = []
# all_fixtures = fixtures.find()
avg_stat_names = []
match_stat_names = []
pairs = []
scale = StandardScaler()
season = 2025


def prepareDataSet(league_id, groups):
    del avg_stat_names[:]
    del match_stat_names[:]
    print('\nCollecting stats...')
    (pairs, all_team_stats) = sc.collect_stats(
        matches, league_id, groups, season)

    # print('Adding recent encounters...')
    # pairs = sc.recent_encounter(pairs)

    print('Calculating average values...')
    (avg_team_stats, avg_team_home_stats,
     avg_team_away_stats, med_team_stats, med_team_home_stats,
     med_team_away_stats, min_team_stats, min_team_home_stats,
     min_team_away_stats, max_team_stats, max_team_home_stats,
     max_team_away_stats) = sc.calculate_average_team_stats(all_team_stats)

    for pair in pairs:
        pair.setdefault('homeTeamAvgStats', {})
        pair.setdefault('awayTeamAvgStats', {})
        pair['homeTeamAvgStats'] = avg_team_stats[pair['homeTeamId']]
        pair['awayTeamAvgStats'] = avg_team_stats[pair['awayTeamId']]
        pair['homeTeamAvgHomeStats'] = avg_team_home_stats[pair['homeTeamId']]
        pair['awayTeamAvgAwayStats'] = avg_team_away_stats[pair['awayTeamId']]

        pair.setdefault('homeTeamMedStats', {})
        pair.setdefault('awayTeamMedStats', {})
        pair['homeTeamMedStats'] = med_team_stats[pair['homeTeamId']]
        pair['awayTeamMedStats'] = med_team_stats[pair['awayTeamId']]
        pair['homeTeamMedHomeStats'] = med_team_home_stats[pair['homeTeamId']]
        pair['awayTeamMedAwayStats'] = med_team_away_stats[pair['awayTeamId']]
#
#        pair.setdefault('homeTeamMinStats', {})
#        pair.setdefault('awayTeamMinStats', {})
#        pair['homeTeamMinStats'] = min_team_stats[pair['homeTeamId']]
#        pair['awayTeamMinStats'] = min_team_stats[pair['awayTeamId']]
#        pair['homeTeamMinHomeStats'] = min_team_home_stats[pair['homeTeamId']]
#        pair['awayTeamMinAwayStats'] = min_team_away_stats[pair['awayTeamId']]
#
#        pair.setdefault('homeTeamMaxStats', {})
#        pair.setdefault('awayTeamMaxStats', {})
#        pair['homeTeamMaxStats'] = max_team_stats[pair['homeTeamId']]
#        pair['awayTeamMaxStats'] = max_team_stats[pair['awayTeamId']]
#        pair['homeTeamMaxHomeStats'] = max_team_home_stats[pair['homeTeamId']]
#        pair['awayTeamMaxAwayStats'] = max_team_away_stats[pair['awayTeamId']]

    print('Flattening data...')
    pairs = sc.flatten_stats(pairs)

    print('Adding prediction groups...')
    pairs = sc.add_groups(pairs, groups)

    print('Adding win/draw/lose statisctics...')
    #pairs = sc.win_draw_lose(pairs)

    print('Adding shape for this season home/away matches...')
    for i, pair in enumerate(pairs):
        thiSeasonMatches = [x for x in pairs if x['season'] == pair['season']]

        ht_match_len = len([x for x in thiSeasonMatches if (
            x['homeTeamId'] == pair['homeTeamId'] and x['season'] == pair['season'])])
        at_match_len = len([x for x in thiSeasonMatches if (
            x['awayTeamId'] == pair['awayTeamId'] and x['season'] == pair['season'])])
        pairs[i] = pair | sc.home_away_shape(
            pairs, pair['fixture'], ht_match_len, at_match_len, True)

    print('Adding shape for 3 home/away last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.home_away_shape(
            pairs, pair['fixture'], 3, 3)

    print('Adding shape for 5 home/away last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.home_away_shape(
            pairs, pair['fixture'], 5, 5)

    print('Adding shape for this season ...')
    for i, pair in enumerate(pairs):
        thiSeasonMatches = [x for x in pairs if x['season'] == pair['season']]

        ht_match_len = len([x for x in thiSeasonMatches if (
            x['homeTeamId'] == pair['homeTeamId'] and x['season'] == pair['season'])])
        at_match_len = len([x for x in thiSeasonMatches if (
            x['awayTeamId'] == pair['awayTeamId'] and x['season'] == pair['season'])])
        pairs[i] = pair | sc.find_last_matches(
            pairs, pair['fixture'], True, True, ht_match_len, at_match_len, True)

    print('Adding shape for 3 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.find_last_matches(
            pairs, pair['fixture'], True, True)

    print('Adding shape for 5 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.find_last_matches(
            pairs, pair['fixture'], True, True, 5, 5)

    print('Adding xg difference shape for this season ...')
    for i, pair in enumerate(pairs):
        thiSeasonMatches = [x for x in pairs if x['season'] == pair['season']]

        ht_match_len = len([x for x in thiSeasonMatches if (
            x['homeTeamId'] == pair['homeTeamId'] and x['season'] == pair['season'])])
        at_match_len = len([x for x in thiSeasonMatches if (
            x['awayTeamId'] == pair['awayTeamId'] and x['season'] == pair['season'])])
        pairs[i] = pair | sc.xg_difference_shape(
            pairs, pair['fixture'], True, True, ht_match_len, at_match_len, True)

    print('Adding xg difference shape for 3 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.xg_difference_shape(
            pairs, pair['fixture'], True, True)

    print('Adding xg difference shape for 5 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.xg_difference_shape(
            pairs, pair['fixture'], True, True, 5, 5)

    print('Adding team stats...')

    def team_stat_calc(teamId, prefix):
        team_info = teams.find_one({'season': 2024, 'team.id': teamId}, {
                                   'statistics': 1, 'team': 1})
        if not team_info:
            team_info = teams.find_one()
        team_stats = team_info['statistics']
        team_storage = {}
        team_storage[f'{prefix} Stat Win Rate'] = team_stats['fixtures']['wins']['total'] / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Draw Rate'] = team_stats['fixtures']['draws']['total'] / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Loses Rate'] = team_stats['fixtures']['loses']['total'] / \
            team_stats['fixtures']['played']['total']

        team_storage[f'{prefix} Stat Home Win Rate'] = team_stats['fixtures']['wins']['home'] / \
            team_stats['fixtures']['played']['home']
        team_storage[f'{prefix} Stat Home Draw Rate'] = team_stats['fixtures']['draws']['home'] / \
            team_stats['fixtures']['played']['home']
        team_storage[f'{prefix} Stat Home Loses Rate'] = team_stats['fixtures']['loses']['home'] / \
            team_stats['fixtures']['played']['home']

        team_storage[f'{prefix} Stat Away Win Rate'] = team_stats['fixtures']['wins']['away'] / \
            team_stats['fixtures']['played']['away']
        team_storage[f'{prefix} Stat Away Draw Rate'] = team_stats['fixtures']['draws']['away'] / \
            team_stats['fixtures']['played']['away']
        team_storage[f'{prefix} Stat Away Loses Rate'] = team_stats['fixtures']['loses']['away'] / \
            team_stats['fixtures']['played']['away']

        team_storage[f'{prefix} Stat Goals Over 0.5'] = team_stats['goals']['for']['under_over']['0.5']['over'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Under 0.5'] = team_stats['goals']['for']['under_over']['0.5']['under'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Over 1.5'] = team_stats['goals']['for']['under_over']['1.5']['over'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Under 1.5'] = team_stats['goals']['for']['under_over']['1.5']['under'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Over 2.5'] = team_stats['goals']['for']['under_over']['2.5']['over'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Under 12.5'] = team_stats['goals']['for']['under_over']['2.5']['under'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Over 23.5'] = team_stats['goals']['for']['under_over']['3.5']['over'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Under 3.5'] = team_stats['goals']['for']['under_over']['3.5']['under'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Over 4.5'] = team_stats['goals']['for']['under_over']['4.5']['over'] / \
            team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals Under 4.5'] = team_stats['goals']['for']['under_over']['4.5']['under'] / \
            team_stats['goals']['for']['total']['total']

        team_storage[f'{prefix} Stat Goals Against Over 0.5'] = team_stats['goals']['against']['under_over']['0.5']['over'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Under 0.5'] = team_stats['goals']['against']['under_over']['0.5']['under'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Over 1.5'] = team_stats['goals']['against']['under_over']['1.5']['over'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Under 1.5'] = team_stats['goals']['against']['under_over']['1.5']['under'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Over 2.5'] = team_stats['goals']['against']['under_over']['2.5']['over'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Under 12.5'] = team_stats['goals']['against']['under_over']['2.5']['under'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Over 23.5'] = team_stats['goals']['against']['under_over']['3.5']['over'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Under 3.5'] = team_stats['goals']['against']['under_over']['3.5']['under'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Over 4.5'] = team_stats['goals']['against']['under_over']['4.5']['over'] / \
            team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against Under 4.5'] = team_stats['goals']['against']['under_over']['4.5']['under'] / \
            team_stats['goals']['against']['total']['total']

        team_storage[f'{prefix} Stat Goals for Minute 0-15'] = (
            team_stats['goals']['for']['minute']['0-15']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 16-30'] = (
            team_stats['goals']['for']['minute']['16-30']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 31-45'] = (
            team_stats['goals']['for']['minute']['31-45']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 46-60'] = (
            team_stats['goals']['for']['minute']['46-60']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 61-75'] = (
            team_stats['goals']['for']['minute']['61-75']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 76-90'] = (
            team_stats['goals']['for']['minute']['76-90']['total'] or 0) / team_stats['goals']['for']['total']['total']
        team_storage[f'{prefix} Stat Goals for Minute 91-105'] = (
            team_stats['goals']['for']['minute']['91-105']['total'] or 0) / team_stats['goals']['for']['total']['total']

        team_storage[f'{prefix} Stat Goals Against for Minute 0-15'] = (
            team_stats['goals']['against']['minute']['0-15']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 16-30'] = (
            team_stats['goals']['against']['minute']['16-30']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 31-45'] = (
            team_stats['goals']['against']['minute']['31-45']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 46-60'] = (
            team_stats['goals']['against']['minute']['46-60']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 61-75'] = (
            team_stats['goals']['against']['minute']['61-75']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 76-90'] = (
            team_stats['goals']['against']['minute']['76-90']['total'] or 0) / team_stats['goals']['against']['total']['total']
        team_storage[f'{prefix} Stat Goals Against for Minute 91-105'] = (
            team_stats['goals']['against']['minute']['91-105']['total'] or 0) / team_stats['goals']['against']['total']['total']

        team_storage[f'{prefix} Stat Goals Against for Minute 0-15 Diff'] = (
            team_stats['goals']['against']['minute']['0-15']['total'] or 0) - (team_stats['goals']['for']['minute']['0-15']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 16-30 Diff'] = (
            team_stats['goals']['against']['minute']['16-30']['total'] or 0) / (team_stats['goals']['for']['minute']['16-30']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 31-45 Diff'] = (
            team_stats['goals']['against']['minute']['31-45']['total'] or 0) / (team_stats['goals']['for']['minute']['31-45']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 46-60 Diff'] = (
            team_stats['goals']['against']['minute']['46-60']['total'] or 0) / (team_stats['goals']['for']['minute']['46-60']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 61-75 Diff'] = (
            team_stats['goals']['against']['minute']['61-75']['total'] or 0) / (team_stats['goals']['for']['minute']['61-75']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 76-90 Diff'] = (
            team_stats['goals']['against']['minute']['76-90']['total'] or 0) / (team_stats['goals']['for']['minute']['76-90']['total'] or 0.001)
        team_storage[f'{prefix} Stat Goals Against for Minute 91-105 Diff'] = (
            team_stats['goals']['against']['minute']['91-105']['total'] or 0) / (team_stats['goals']['for']['minute']['91-105']['total'] or 0.001)

        team_storage[f'{prefix} Stat Clean Sheet Home'] = team_stats['clean_sheet']['home'] / \
            team_stats['fixtures']['played']['home']
        team_storage[f'{prefix} Stat Clean Sheet Away'] = team_stats['clean_sheet']['away'] / \
            team_stats['fixtures']['played']['away']
        team_storage[f'{prefix} Stat Clean Sheet Total'] = team_stats['clean_sheet']['total'] / \
            team_stats['fixtures']['played']['total']

        team_storage[f'{prefix} Stat Failed to Score Home'] = team_stats['failed_to_score']['home'] / \
            team_stats['fixtures']['played']['home']
        team_storage[f'{prefix} Stat Failed to Score Away'] = team_stats['failed_to_score']['away'] / \
            team_stats['fixtures']['played']['away']
        team_storage[f'{prefix} Stat Failed to Score Total'] = team_stats['failed_to_score']['total'] / \
            team_stats['fixtures']['played']['total']

        #print(team_stats['cards']['yellow'])
        team_storage[f'{prefix} Stat Cards Yellow for Minute 0-15'] = (team_stats['cards']['yellow']['0-15']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 16-30'] = (team_stats['cards']['yellow']['16-30']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 31-45'] = (team_stats['cards']['yellow']['31-45']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 46-60'] = (team_stats['cards']['yellow']['46-60']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 61-75'] = (team_stats['cards']['yellow']['61-75']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 76-90'] = (team_stats['cards']['yellow']['76-90']['total'] or 0) / \
            team_stats['fixtures']['played']['total']
        team_storage[f'{prefix} Stat Cards Yellow for Minute 91-105'] = (team_stats['cards']['yellow']['91-105']['total'] or 0) / \
            team_stats['fixtures']['played']['total']

        team_storage[f'{prefix} Stat Biggest Win Streak'] = team_stats['biggest']['streak']['wins']
        team_storage[f'{prefix} Stat Biggest Win Draws'] = team_stats['biggest']['streak']['draws']
        team_storage[f'{prefix} Stat Biggest Win Loses'] = team_stats['biggest']['streak']['loses']

        buff = team_stats['biggest']['wins']['home'].split(
            '-') if team_stats['biggest']['wins']['home'] != None else [0, 0]
        team_storage[f'{prefix} Stat Biggest Win Goal Diff Home'] = int(
            buff[0]) - int(buff[1])
        buff = team_stats['biggest']['wins']['away'].split(
            '-') if team_stats['biggest']['wins']['away'] != None else [0, 0]
        team_storage[f'{prefix} Stat Biggest Win Goal Diff Away'] = int(
            buff[0]) - int(buff[1])
        buff = team_stats['biggest']['loses']['home'].split(
            '-') if team_stats['biggest']['loses']['home'] != None else [0, 0]
        team_storage[f'{prefix} Stat Biggest Loses Goal Diff Home'] = int(
            buff[0]) - int(buff[1])
        buff = team_stats['biggest']['loses']['away'].split(
            '-') if team_stats['biggest']['loses']['away'] != None else [0, 0]
        team_storage[f'{prefix} Stat Biggest Loses Goal Diff Away'] = int(
            buff[0]) - int(buff[1])

        team_storage[f'{prefix} Stat Biggest Home Goals'] = team_stats['biggest']['goals']['for']['home']
        team_storage[f'{prefix} Stat Biggest Away Goals'] = team_stats['biggest']['goals']['for']['away']
        team_storage[f'{prefix} Stat Biggest Home Goals Against'] = team_stats['biggest']['goals']['against']['home']
        team_storage[f'{prefix} Stat Biggest Away Goals Against'] = team_stats['biggest']['goals']['against']['away']

        return team_storage

    for i, pair in enumerate(pairs):
        ht_stats = team_stat_calc(pair['homeTeamId'], 'Home Team')
        at_stats = team_stat_calc(pair['awayTeamId'], 'Away Team')
        pairs[i] = pair | ht_stats | at_stats

    df = pandas.DataFrame.from_dict(
        [pair for pair in pairs if not 'awayTeamAvgStats' in pair
            and not 'homeTeamAvgStats' in pair
            and not 'awayTeamMedStats' in pair
            and not 'homeTeamMedStats' in pair
            and not 'awayTeamMinStats' in pair
            and not 'homeTeamMInStats' in pair
            and not 'awayTeamMaxStats' in pair
            and not 'homeTeamMaxStats' in pair])
    df.columns = df.columns.to_series().apply(lambda x: x.strip())
    df.reset_index(drop=True)

    if len(avg_stat_names) == 0:
        for statName in pairs[0]:
            if 'Avg' in statName and not 'goals_prevented' in statName and not 'is_home' in statName and not 'Team Stat' in statName:
                avg_stat_names.append(statName)
            if 'Shape' in statName:
                avg_stat_names.append(statName)
            if 'Recent' in statName:
                avg_stat_names.append(statName)
            if 'Stat' in statName:
                avg_stat_names.append(statName)
            if 'Med' in statName:
                avg_stat_names.append(statName)
#            #if 'Min' in statName:
#            #    avg_stat_names.append(statName)
#            if 'Max' in statName:
#                avg_stat_names.append(statName)
        for statName in avg_stat_names:
            match_stat_names.append(statName.replace(
                ' Avg', '').replace(' Against', ''))
            # match_stat_names.append(statName.replace(
            #    ' Med', '').replace(' edian Against', ''))
            # match_stat_names.append(statName.replace(
            #    ' Min', '').replace(' inimum Against', ''))
            # match_stat_names.append(statName.replace(
            #    ' Max', '').replace(' aximum Against', ''))
    return (df, pairs)


def get_shape(home_team, away_team, pairs, fixture=None):
    away_team_form = {}
    home_team_form = {}
    last_occur = {}

    if fixture:
        last_occur = [x for x in pairs[::-1] if x['awayTeamId'] ==
                      away_team or x['homeTeamId'] == away_team and x['fixture'] < fixture][0]
    else:
        last_occur = [x for x in pairs[::-1] if x['awayTeamId']
                      == away_team or x['homeTeamId'] == away_team][0]

    thiSeasonMatches = [
        x for x in pairs if x['season'] == last_occur['season']]

    ht_match_len = len([x for x in thiSeasonMatches if (
        x['homeTeamId'] == home_team and x['season'] == last_occur['season'])])
    at_match_len = len([x for x in thiSeasonMatches if (
        x['awayTeamId'] == away_team and x['season'] == last_occur['season'])])

    if last_occur['homeTeamId'] == away_team:
        away_team_form_reversed = {**sc.find_last_matches(
            pairs, last_occur['fixture'], True, False), **sc.find_last_matches(
            pairs, last_occur['fixture'], True, False, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, ht_match_len, at_match_len, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], True, False, ht_match_len, at_match_len, True)}
        for stat in away_team_form_reversed:
            if 'Home' in stat:
                away_team_form[stat.replace(
                    'Home', 'Away')] = away_team_form_reversed[stat]
            elif 'Away' in stat:
                away_team_form[stat.replace(
                    'Away', 'Home')] = away_team_form_reversed[stat]
    else:
        away_team_form = {**sc.find_last_matches(
            pairs, last_occur['fixture'], False, True), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], False, True, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, ht_match_len, at_match_len, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], False, True, ht_match_len, at_match_len, True)}

    last_occur = [x for x in pairs[::-1] if x['awayTeamId']
                  == home_team or x['homeTeamId'] == home_team][0]
    if last_occur['awayTeamId'] == home_team:
        home_team_form_reversed = {**sc.find_last_matches(
            pairs, last_occur['fixture'], False, True), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], False, True, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, ht_match_len, at_match_len, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], False, True, ht_match_len, at_match_len, True)}
        for stat in home_team_form_reversed:
            if 'Home' in stat:
                home_team_form[stat.replace(
                    'Home', 'Away')] = home_team_form_reversed[stat]
            elif 'Away' in stat:
                home_team_form[stat.replace(
                    'Away', 'Home')] = home_team_form_reversed[stat]
    else:
        home_team_form = {**sc.find_last_matches(
            pairs, last_occur['fixture'], True, False), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False), **sc.find_last_matches(
            pairs, last_occur['fixture'], True, False, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, 5, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, ht_match_len, at_match_len, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], True, False, ht_match_len, at_match_len, True)}

    return away_team_form | home_team_form | sc.home_away_shape(
        pairs, last_occur['fixture']) | sc.home_away_shape(
        pairs, last_occur['fixture'], 5, 5) | sc.home_away_shape(
        pairs, last_occur['fixture'], ht_match_len, at_match_len, True)


def predict(df, pairs, home_team, away_team, stat_to_predict, deps, rate, is_test, scaler, regr):
    data = []
    homeTeam = df.loc[(df['homeTeamId'] == home_team)].iloc[0]
    awayTeam = df.loc[(df['awayTeamId'] == away_team)].iloc[0]
    newdf = df.loc[(df['homeTeamId'] == home_team) |
                   (df['awayTeamId'] == away_team)]

    team_shape = get_shape(home_team, away_team,
                           pairs, None)

    for dep in deps:
        if 'Avg' in dep:
            if 'Home' in dep:
                data.append(homeTeam[dep])
            else:
                data.append(awayTeam[dep])
        if 'Stat ' in dep:
            if 'Home' in dep:
                data.append(homeTeam[dep])
            else:
                data.append(awayTeam[dep])
        elif 'Shape' in dep:
            data.append(team_shape[dep])
        elif 'Med' in dep:
            if 'Home' in dep:
                data.append(homeTeam[dep])
            else:
                data.append(awayTeam[dep])
#        #elif 'Min' in dep:
#        #    data.append(team_shape[dep])
#        elif 'Max' in dep:
#            if 'Home' in dep:
#                data.append(homeTeam[dep])
#            else:
#                data.append(awayTeam[dep])

    X = newdf[deps]
    y = newdf[stat_to_predict]
    calced = pd.predictStats(X, y, pandas.DataFrame(
        [data], columns=deps), scaler, regr)
    print(stat_to_predict, ':', float(
        calced[0]) // 0.01 / 100, f"({rate})", deps if len(deps) <= 10 else f"{len(deps)} parameters")
    return float(calced[0])


def rate(df, income_stat, calc_stat, home_team, away_team):
    X = df[income_stat]
    y = df[calc_stat]
    return pd.sss_train_test_model(X, y)


def rate_selected_features(df, income_stat, calc_stat, home_team, away_team):

    # df.to_excel("dataset.xlsx")
    X = df[income_stat]
    y = df[calc_stat]
    scaledX = scale.fit_transform(X)
    return pd.select_features(pandas.DataFrame(scaledX, columns=income_stat), y)


def select_logistic_features(df, income_stat, calc_stat, home_team, away_team):
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)]

    # df.to_excel("dataset.xlsx")
    X = newdf[income_stat]
    y = newdf[calc_stat]
    scaledX = scale.fit_transform(X)
    return pd.select_logistic_features(pandas.DataFrame(scaledX, columns=income_stat), y)


predicted_stats = [
    'Shots on Goal',
    'Shots off Goal',
    'Total Shots',
    'Blocked Shots',
    'Shots insidebox',
    'Shots outsidebox',
    # 'Fouls',
    # 'Yellow Cards',
    # 'Red Cards',
    # 'Corner Kicks',
    # 'Offsides',
    # 'Ball Possession',
    # 'Goalkeeper Saves',
    # 'Total passes',
    # 'Passes accurate',
    # 'Passes %',
    # 'expected_goals',
    # 'Goals',
]


def filter_features(df, pairs, home_team, away_team, statName):
    # move outside
    print('Selecting features for', statName)
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)].fillna(0)

    deps = [x for x in avg_stat_names if (
        # 'Odd' in x or
        'Avg' in x or
        'Stat' in x or
        # 'Med' in x or
        # 'Min' in x or
        'Max' in x or
        'Shape' in x)
        # and not 'Odd' in x
        and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    deps, score = rate_selected_features(
        newdf, deps, statName, home_team, away_team)
    return deps


def predict_filtered(df, pairs, home_team, away_team, statName, deps):
    rated = rate(df, deps, statName, home_team, away_team)[0] // 0.01 / 100
    predicted = predict(df, pairs, home_team, away_team, statName,
                        deps, rated // 0.01 / 100, False, None, None)

    return (predicted, rated, deps)


def predictAll(df, pairs, home_team, away_team, statName, accuracy=6):
    results = []
    resultNames = []
    deps_length = int(accuracy) * 3
    # move outside
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)].fillna(0)
    deps = [x for x in avg_stat_names if (
        'Odd' in x or
        'Avg' in x or
        'Stat' in x or
        'Med' in x or
        # 'Min' in x or
        'Max' in x or
        'Shape' in x)
        # and not 'Odd' in x
        and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    print('All deps: ', len(deps))
    deps, score = rate_selected_features(
        newdf, deps, statName, home_team, away_team)
    print('Selected deps: ', len(deps))

    # best_deps = sorted(deps, reverse=True, key=lambda x: rate(newdf,
    #                                                          [x], statName, home_team, away_team)[0])
    # num_of_deps = len(best_deps[:deps_length][:int(accuracy)])
    #
    # for i in range(num_of_deps):
    #    set_of_stats = [list(x) for x in (combinations(best_deps[:deps_length], i+1))]
    #    for index, combination in enumerate(set_of_stats):
    #        resultNames.append(combination)
    #        results.append(
    #            rate(newdf, combination, statName, home_team, away_team))

    combination = deps
    resultNames.append(combination)
    results.append(
        rate(df, combination, statName, home_team, away_team))

    index_max = [x[0] for x in results].index(max([x[0] for x in results]))
    predicted = predict(df, pairs, home_team, away_team, statName,
                        resultNames[index_max], results[index_max][0] // 0.01 / 100, False, results[index_max][1], results[index_max][2])

    return (predicted, results[index_max][0] // 0.01 / 100, resultNames[index_max])


print('Ready to use.')


def find_task(id):
    try:
        df = None
        match = queue.find_one({'_id': ObjectId(id)})
        queue.update_one({'_id': ObjectId(id)}, {
                         '$set': {'status': 'in_progress'}})
        first_line = True
        home_team = fixtures.find_one({'teams.home.id': match['homeTeam']['id']})[
            'teams']['home']
        away_team = fixtures.find_one({'teams.away.id': match['awayTeam']['id']})[
            'teams']['away']
        print('\n', home_team['name'], away_team['name'])
        match_coeff = {}
        db_predicted = {}
        db_predicted['odds'] = {}
        accuracy = 3

        predicted_groups = predict_task(
            home_team, away_team, accuracy, match['_id'])

        for predicted_group in predicted_groups:
            match_coeff['homeTeam'] = match['homeTeam']
            match_coeff['awayTeam'] = match['awayTeam']
            db_predicted['homeTeam'] = home_team['name']
            db_predicted['userId'] = int(match['userId'])
            db_predicted['tags'] = match['tag']
            db_predicted['awayTeam'] = away_team['name']
            db_predicted['league'] = fixtures.find_one(
                {'teams.home.id': home_team['id']})['league']['name']
            # db_predicted['hints'] =
            db_predicted['odds'][predicted_group['betName']] = {
                'hints': predicted_group['hints'],
                'relative': [
                    1 / min_value(x, 0.001) // 0.001 / 1000 for x in predicted_group['relative_odds']],
                'absolute': [
                    1 / min_value(x, 0.001) // 0.001 / 1000 for x in predicted_group['absolute_odds']],
                'rates': predicted_group['rates'],
                'relative_rates': predicted_group['relative_rates']
            }
            match_coeff[predicted_group['betName']] = [[
                1 / min_value(x, 0.001) // 0.01 / 100 for x in predicted_group['relative_odds']], predicted_group['avg_accuracy']]
        predicted.insert_one(db_predicted)
        if first_line:
            first_line = False
            df = pandas.DataFrame([match_coeff])
        else:
            df = df._append(match_coeff, ignore_index=True)
        # df.to_excel("odds_snapshot.xlsx")
        queue.delete_one({'_id': ObjectId(id)})
        # queue.update_one({'_id': ObjectId(id)}, {
        #                 '$set': {'status': 'finished'}})
    except Exception:
        print(traceback.format_exc())
        queue.update_one({'_id': ObjectId(id)}, {'$set': {'status': 'failed'}})

# def predict_task(home_team, away_team, accuracy, match_id):
#    home_team = home_team['id']
#    away_team = away_team['id']
#    groups = queue.find_one({'_id': ObjectId(match_id)})['groups']
#
#    df, pairs = prepareDataSet(matches.find_one(
#        {'homeTeam.team.id': home_team})['league'], groups)
#    all_group_results = []
#    for i, group in enumerate(groups):
#        print(f'\nPredicting group <{group["name"]}>')
#        parts = []
#        accuracies = []
#        relative_odds = []
#        hints = []
#        full_deps = []
#        relative_rates = []
#        for odd in group['items']:
# print(f'Predicting odd <{odd["name"]}>')
##
# predicted, accuracy_resulted, deps = predictAll(
# df, pairs, home_team, away_team, odd['name'], accuracy)
# parts.append(predicted)
# accuracies.append(accuracy_resulted)
# hints.append(odd['name'])
# full_deps = list(set([*full_deps, *list(dict.fromkeys(deps))]))
#            full_deps =  list(set([*full_deps, *list(dict.fromkeys(filter_features(df, pairs, home_team, away_team, odd["name"])))]))
#        for odd in group['items']:
#            print(f'\nPredicting relative odd <{odd["name"]}>')
#
#
#            relative_odds.append(predict_filtered(df, pairs, home_team, away_team, odd["name"], full_deps))
#        all_group_results.append({
#            'betName': group['name'],
#            'hints': hints,
#            'avg_accuracy': statistics.mean([x[1] for x in relative_odds]),
#            'rates': [x[1] for x in relative_odds],
#            #'absolute_odds': parts,
#            'absolute_odds': [x[0] for x in relative_odds],
#            # 'alt_relative_odds': expand_odds(parts),
#            # 'alt_relative_rates': accuracies,
#            'relative_rates': [x[1] for x in relative_odds],
#            'relative_odds': expand_odds([x[0] for x in relative_odds]),
#        })
#    return all_group_results


def predict_task(home_team, away_team, accuracy, match_id):
    home_team = home_team['id']
    away_team = away_team['id']
    groups = queue.find_one({'_id': ObjectId(match_id)})['groups']

    df, pairs = prepareDataSet(matches.find_one(
        {'homeTeam.team.id': home_team})['league'], groups)
    all_group_results = []
    for i, group in enumerate(groups):
        print(f'\nPredicting group <{group["name"]}>')
        parts = []
        accuracies = []
        relative_odds = []
        hints = []
        full_deps = []
        relative_rates = []
        for odd in group['items']:
            print(f'\nPredicting odd <{odd["name"]}>')

            predicted, accuracy_resulted, deps = predictAll(
                df, pairs, home_team, away_team, odd['name'], accuracy)
            parts.append(predicted)
            accuracies.append(accuracy_resulted)
            hints.append(odd['name'])
            full_deps = list(set([*full_deps, *list(dict.fromkeys(deps))]))
        for odd in group['items']:
            print(f'\nPredicting relative odd <{odd["name"]}>')

            relative_rate = rate(df, full_deps,
                                 odd['name'], home_team, away_team)[0]
            relative_odds.append(predict(df, pairs, home_team, away_team, odd['name'],
                                         full_deps, relative_rate, False, None, None))
            relative_rates.append(relative_rate)
        all_group_results.append({
            'betName': group['name'],
            'hints': hints,
            'avg_accuracy': statistics.mean(accuracies),
            'rates': accuracies,
            # 'absolute_odds': parts,
            'absolute_odds': relative_odds,
            # 'alt_relative_odds': expand_odds(parts),
            # 'alt_relative_rates': accuracies,
            'relative_rates': relative_rates,
            'relative_odds': expand_odds(relative_odds),
        })
        items_only_in_list1 = list(set(avg_stat_names).difference(set(full_deps)))
        #for line in items_only_in_list1:
        #    print(line)
        
    return all_group_results


print("Redis Subscriber")

r = redis.Redis(host='host.docker.internal', port=6379, decode_responses=True)
queue_name = "task_queue"
worker_name = f"predictor-{os.getpid()}"


def process_task(task):
    print(f"{worker_name} processing {task}")
    # Synchronous long-running task
    find_task(task['task_id'])
    print(f"{worker_name} finished {task}")


def worker_loop():
    while True:
        # BRPOP blocks until a new task is available
        item = r.brpop(queue_name, timeout=1)
        if item:
            _, task_json = item
            task = json.loads(task_json)
            print(f"{worker_name} got {task}")
            # Synchronously process the task
            process_task(task)
        else:
            time.sleep(0.1)


worker_loop()
