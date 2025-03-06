import json
from websockets.asyncio.server import serve
import pymongo
import stat_calculator as sc
import predictor as pd
import pandas
from sklearn.preprocessing import StandardScaler
from itertools import combinations
from multiprocessing.pool import Pool
import asyncio
import websockets
import dataset as ds
import statistics
from bson.objectid import ObjectId

myclient = pymongo.MongoClient(
    'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
matches = myclient["statistics"]["matches"]
fixtures = myclient["statistics"]["fixtures"]
relations = myclient["statistics"]["relations"]
queue = myclient["statistics"]["queue"]
all_fixtures = fixtures.find()

homeTeamStatNames = []
awayTeamStatNames = []
avg_stat_names = []
match_stat_names = []
pairs = []
scale = StandardScaler()


def prepareDataSet(league_id, groups):
    print('Collecting stats...')
    (pairs, all_team_stats) = sc.collect_stats(matches, league_id, groups)

    print('Calculating difference...')
    pairs = sc.difference(pairs, all_team_stats)

    print('Calculating average values...')
    (avg_team_stats, avg_team_home_stats,
     avg_team_away_stats) = sc.calculate_average_team_stats(all_team_stats)

    for pair in pairs:
        pair.setdefault('homeTeamAvgStats', {})
        pair.setdefault('awayTeamAvgStats', {})
        pair['homeTeamAvgStats'] = avg_team_stats[pair['homeTeamId']]
        pair['awayTeamAvgStats'] = avg_team_stats[pair['awayTeamId']]
        pair['homeTeamAvgHomeStats'] = avg_team_home_stats[pair['homeTeamId']]
        pair['awayTeamAvgAwayStats'] = avg_team_away_stats[pair['awayTeamId']]

    print('Flattening data...')
    pairs = sc.flatten_stats(pairs)

    print('Adding prediction groups...')
    pairs = sc.add_groups(pairs, groups)

    print('Adding win/draw/lose statisctics...')
    pairs = sc.win_draw_lose(pairs)

    print('Adding statisctic shape for 3 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.find_last_matches(
            pairs, pair['fixture'], True, True)
    print('Adding statisctic shape for 5 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.find_last_matches(
            pairs, pair['fixture'], True, True, 5)
    print('Adding xg difference shape for 3 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.xg_difference_shape(
            pairs, pair['fixture'], True, True)
    print('Adding xg difference shape for 5 last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.xg_difference_shape(
            pairs, pair['fixture'], True, True, 5)

    # for statName in pairs[0]:
    #    if 'Home Team' in statName and not 'Name' in statName and not 'Avg' in statName:
    #        homeTeamStatNames.append(statName)
    #    elif 'Away Team' in statName and not 'Name' in statName and not 'Avg' in statName:
    #        awayTeamStatNames.append(statName)

    df = pandas.DataFrame.from_dict(
        [pair for pair in pairs if not 'awayTeamAvgStats' in pair and not 'homeTeamAvgStats' in pair])
    df.columns = df.columns.to_series().apply(lambda x: x.strip())
    df.reset_index(drop=True)

    for statName in pairs[0]:
        if 'Avg' in statName and not 'goals_prevented' in statName and not 'is_home' in statName:
            avg_stat_names.append(statName)
        if 'Shape' in statName:
            avg_stat_names.append(statName)

    for statName in avg_stat_names:
        match_stat_names.append(statName.replace(
            ' Avg', '').replace(' Against', ''))
    return (df, pairs)


def get_shape(home_team, away_team, pairs, fixture=None):
    away_team_form = {}
    home_team_form = {}
    last_occur = {}
    if fixture:
        last_occur = [x for x in pairs[::-1] if x['awayTeamId'] ==
                      away_team or x['homeTeamId'] == away_team and x['fixture'] < fixture][0]
    else:
        print(home_team, away_team, len(pairs))
        last_occur = [x for x in pairs[::-1] if x['awayTeamId']
                      == away_team or x['homeTeamId'] == away_team][0]
    if last_occur['homeTeamId'] == away_team:
        away_team_form_reversed = {**sc.find_last_matches(
            pairs, last_occur['fixture'], True, False), **sc.find_last_matches(
            pairs, last_occur['fixture'], True, False, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, 5)}
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
            pairs, last_occur['fixture'], False, True, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, 5)}

    last_occur = [x for x in pairs[::-1] if x['awayTeamId']
                  == home_team or x['homeTeamId'] == home_team][0]
    if last_occur['awayTeamId'] == home_team:
        home_team_form_reversed = {**sc.find_last_matches(
            pairs, last_occur['fixture'], False, True), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True), **sc.find_last_matches(
            pairs, last_occur['fixture'], False, True, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], False, True, 5)}
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
            pairs, last_occur['fixture'], True, False, 5), **sc.xg_difference_shape(
            pairs, last_occur['fixture'], True, False, 5)}
    return away_team_form | home_team_form


def prepare_X(home_team_name, away_team_name, deps, is_test=None):
    data = []
    homeTeam = df.loc[(df['homeTeamId'] == home_team_name)].iloc[-1]
    awayTeam = df.loc[(df['awayTeamId'] == away_team_name)].iloc[-1]
    newdf = df.loc[(df['homeTeamId'] == home_team_name) ^
                   (df['awayTeamId'] == away_team_name)]

    team_shape = get_shape(home_team_name, away_team_name,
                           pairs, None)

    numeric_cols = deps
    for dep in numeric_cols:
        if 'Avg' in dep and not 'Shape' in dep:
            if 'Home' in dep:
                data.append(homeTeam[dep])
            else:
                data.append(awayTeam[dep])
        elif 'Shape' in dep:
            data.append(team_shape[dep])
    return pandas.DataFrame([data], columns=numeric_cols)


def predict(df, pairs, home_team_name, away_team_name, stat_to_predict, deps, rate, is_test, scaler=StandardScaler()):
    data = []
    homeTeam = df.loc[(df['homeTeamId'] == home_team_name)].iloc[0]
    awayTeam = df.loc[(df['awayTeamId'] == away_team_name)].iloc[0]
    newdf = df.loc[(df['homeTeamId'] == home_team_name) ^
                   (df['awayTeamId'] == away_team_name)]

    team_shape = get_shape(home_team_name, away_team_name,
                           pairs, None)

    for dep in deps:
        if 'Avg' in dep:
            if 'Home' in dep:
                data.append(homeTeam[dep])
            else:
                data.append(awayTeam[dep])
        elif 'Shape' in dep:
            data.append(team_shape[dep])

    X = newdf[deps]
    y = newdf[stat_to_predict]
    calced = pd.predictStats(X, y, pandas.DataFrame([data], columns=deps))
    print(stat_to_predict, ':', calced[0] // 0.01 / 100, f"({rate})", deps)
    return calced[0]


def rate(df, income_stat, calc_stat, home_team, away_team):
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)]
    X = newdf[income_stat]
    y = newdf[calc_stat]
    return pd.train_test_model(X, y)[0]


def rate_selected_features(df, income_stat, calc_stat, home_team, away_team):
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)]
    X = newdf[income_stat]
    y = newdf[calc_stat]
    scaledX = scale.fit_transform(X)
    return pd.select_features(pandas.DataFrame(scaledX, columns=income_stat), y)


predicted_stats = [
    'Shots on Goal',
    'Shots off Goal',
    'Blocked Shots',
    'Shots insidebox',
    'Shots outsidebox',
    'Fouls',
    'Yellow Cards',
    'Red Cards',
    'Corner Kicks',
    'Offsides',
    'Ball Possession',
    'Goalkeeper Saves',
    'Total passes',
    'Passes accurate',
    'Passes %',
    'expected_goals',
    'Goals',
]


def predictAll(df, pairs, home_team, away_team, statName, accuracy=2):
    results = []
    resultNames = []
    deps_length = int(accuracy) * 2
    deps = [x for x in avg_stat_names if (
        'Avg' in x or
        'Shape' in x) and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    deps, score = rate_selected_features(
        df, deps, statName, home_team, away_team)
    best_deps = sorted(deps, reverse=True, key=lambda x: rate(df,
                                                              [x], statName, home_team, away_team))[:deps_length]
    num_of_deps = len(best_deps[:int(accuracy)])

    for i in range(num_of_deps):
        set_of_stats = [list(x) for x in (combinations(best_deps, i+1))]
        for index, combination in enumerate(set_of_stats):
            resultNames.append(combination)
            results.append(
                rate(df, combination, statName, home_team, away_team))

    index_max = results.index(max(results))
    predicted = predict(df, pairs, home_team, away_team, statName,
                        resultNames[index_max], results[index_max] // 0.01 / 100, False)
    return (predicted, results[index_max], resultNames[index_max])


def avoid_zero_value(val, zero=0.01):
    return zero if val == 0 else val


def max_value(val, max=0.99):
    return max if val >= 1 else val


def min_value(val, min=0.01):
    return min if val <= 0 else val


def calibrate_chanses(parts, original_accuracies):
    accuracies = [0.5 for x in original_accuracies]
    total_parts = sum(parts)
    total_accuracies = sum(accuracies)
    if total_accuracies == 0:
        return parts
    exceeded = 1 - total_parts
    relative_distribution = [(part / total_parts) for part in parts]
    relative_fails = [((1 - accuracy) / total_accuracies)
                      for accuracy in accuracies]
    fail_distribution = [(fail / sum(relative_fails))
                         for fail in relative_fails]
    parts_to_add = [(fail_chance + relative_distribution[i]) / 2 * exceeded for i,
                    fail_chance in enumerate(fail_distribution)]
    distributed_chanses = [avoid_zero_value(
        (part + parts_to_add[i]) // 0.01 / 100) for i, part in enumerate(parts)]
    for i, chance in enumerate(distributed_chanses):
        if chance < 0:
            distributed_chanses[i] = 0.01
            accuracies[i] = 1
            distributed_chanses = calibrate_chanses(
                distributed_chanses, accuracies)
            break
    return distributed_chanses


print('Ready to use.')


async def echo(websocket):
    async for message in websocket:
        d = message.split(">")
        d[0] = int(d[0])
        d[1] = int(d[1])
        groups = []

        df, pairs = prepareDataSet(matches.find_one(
            {'homeTeam.team.id': d[0]})['league'], groups)
        if d[2] == 'stats':
            for statName in ['Home Team ' + x for x in predicted_stats]:
                homeTeamStat, accuracy, deps = predictAll(
                    df, pairs, d[0], d[1], statName, d[3])
                awayTeamStat, accuracy, deps = predictAll(df, pairs, d[0], d[1], statName.replace(
                    'Home', 'Away'), d[3])
                await websocket.send(json.dumps({
                    'mode': d[2],
                    'done': True,
                    'statName': statName.replace('Home Team', ' '),
                    'homeTeam': homeTeamStat if float(homeTeamStat) > 0 else 0.01,
                    'awayTeam': awayTeamStat if float(awayTeamStat) > 0 else 0.01
                }))
                await asyncio.sleep(1)
        else:
            for i, group in enumerate(groups):
                parts = []
                accuracies = []
                hints = []
                for odd in group['items']:
                    # model, deps, scaler = pd.train_test(df, stat=betName)
                    predicted, accuracy, deps = predictAll(
                        df, pairs, d[0], d[1], odd['name'], d[3])
                    parts.append(min_value(max_value(predicted)))
                    accuracies.append(accuracy)
                    hints.append({'odd': odd['name'], 'dependencies': deps})
                await websocket.send(json.dumps({
                    'mode': d[2],
                    'done': False if i < (len(groups) - 1) else True,
                    'betName': group['name'],
                    'hints': hints,
                    'avg_accuracy': statistics.mean(accuracies),
                    'rates': accuracies,
                    'absolute_odds': parts,
                    'relative_odds': calibrate_chanses(parts, accuracies)
                }))
                await asyncio.sleep(1)

#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['h_win', 'h_draw', 'h_lose']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': False,
#                'betName': '1st half winner',
#                'hints': hints,
#                'avg_accuracy': statistics.mean(accuracies),
#                'rates': accuracies,
#                'absolute_odds': parts,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
#            await asyncio.sleep(1)
#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['2h_win', '2h_draw', '2h_lose']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': False,
#                'betName': '2nd half winner',
#                'avg_accuracy': statistics.mean(accuracies),
#                'rates': accuracies,
#                'hints': hints,
#                'absolute_odds': parts,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
#            await asyncio.sleep(1)
#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['win', 'draw', 'lose']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': True,
#                'betName': 'Full time winner',
#                'avg_accuracy': statistics.mean(accuracies),
#                'rates': accuracies,
#                'hints': hints,
#                'absolute_odds': parts,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
#            await asyncio.sleep(1)
#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['1h_more_goals', 'both_halfs_same_goals', '2h_more_goals']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': False,
#                'betName': 'Score by halves',
#                'avg_accuracy': statistics.mean(accuracies),
#                'rates': accuracies,
#                'hints': hints,
#                'absolute_odds': parts,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
#            await asyncio.sleep(1)
#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['ht_first_half', 'ht_equal', 'ht_second_half']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': False,
#                'betName': 'Home team goal distribution',
#                'avg_accuracy': statistics.mean(accuracies),
#                'absolute_odds': parts,
#                'rates': accuracies,
#                'hints': hints,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
#            await asyncio.sleep(1)
#            parts = []
#            accuracies = []
#            hints = []
#            for betName in ['at_first_half', 'at_equal', 'at_second_half']:
#                # model, deps, scaler = pd.train_test(df, stat=betName)
#                predicted, accuracy, deps = predictAll(df, pairs, d[0], d[1], betName, d[3])
#                parts.append(min_value(max_value(predicted)))
#                accuracies.append(accuracy)
#                hints.append({'odd': betName, 'dependencies': deps})
#            await websocket.send(json.dumps({
#                'mode': d[2],
#                'done': True,
#                'betName': 'Away team goal distribution',
#                'avg_accuracy': statistics.mean(accuracies),
#                'rates': accuracies,
#                'hints': hints,
#                'absolute_odds': parts,
#                'relative_odds': calibrate_chanses(parts, accuracies)
#            }))
       # await websocket.send(message)

allowed_origins = ["http://localhost:5173"]


async def main():
    async with serve(echo, "localhost", 8357, ping_interval=None) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
