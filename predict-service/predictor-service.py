import traceback
import redis
import json
import stat_calculator as sc
import predictor as pd
import pandas
from sklearn.preprocessing import StandardScaler
from itertools import combinations
import statistics
from bson.objectid import ObjectId
from services.db import matches, fixtures, queue, predicted
from services.numbers import calibrate_chanses, min_value, max_value, avoid_zero_value, expand_odds

all_fixtures = fixtures.find()
avg_stat_names = []
match_stat_names = []
pairs = []
scale = StandardScaler()


def prepareDataSet(league_id, groups):
    print('\nCollecting stats...')
    (pairs, all_team_stats) = sc.collect_stats(matches, league_id, groups)

    # print('Adding recent encounters...')
    # pairs = sc.recent_encounter(pairs)

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

    print('Adding statisctic shape for # home/away last matches...')
    for i, pair in enumerate(pairs):
        pairs[i] = pair | sc.home_away_shape(
            pairs, pair['fixture'])

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

    df = pandas.DataFrame.from_dict(
        [pair for pair in pairs if not 'awayTeamAvgStats' in pair and not 'homeTeamAvgStats' in pair])
    df.columns = df.columns.to_series().apply(lambda x: x.strip())
    df.reset_index(drop=True)

    for statName in pairs[0]:
        if 'Avg' in statName and not 'goals_prevented' in statName and not 'is_home' in statName:
            avg_stat_names.append(statName)
        if 'Shape' in statName:
            avg_stat_names.append(statName)
        if 'Recent' in statName:
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
    return away_team_form | home_team_form | sc.home_away_shape(
        pairs, last_occur['fixture'])


def predict(df, pairs, home_team, away_team, stat_to_predict, deps, rate, is_test, scaler, regr):
    data = []
    df = df.fillna(0)
    homeTeam = df.loc[(df['homeTeamId'] == home_team)].iloc[0]
    awayTeam = df.loc[(df['awayTeamId'] == away_team)].iloc[0]
    newdf = df.loc[(df['homeTeamId'] == home_team) ^
                   (df['awayTeamId'] == away_team)]

    team_shape = get_shape(home_team, away_team,
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
    calced = pd.predictStats(X, y, pandas.DataFrame(
        [data], columns=deps), scaler, regr)
    print(stat_to_predict, ':', float(
        calced[0]) // 0.01 / 100, f"({rate})", deps)
    return float(calced[0])


def rate(df, income_stat, calc_stat, home_team, away_team):
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)].fillna(0)

    X = newdf[income_stat]
    y = newdf[calc_stat]
    return pd.train_test_model(X, y)


def rate_selected_features(df, income_stat, calc_stat, home_team, away_team):
    newdf = df[(df['homeTeamId'] == home_team) ^
               (df['awayTeamId'] == away_team)]

    # df.to_excel("dataset.xlsx")
    X = newdf[income_stat]
    y = newdf[calc_stat]
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
    #'expected_goals',
    'Goals',
]


def predictAll(df, pairs, home_team, away_team, statName, accuracy=6):
    results = []
    resultNames = []
    deps_length = 20  # int(accuracy) * 3
    deps = [x for x in avg_stat_names if (
        'Odd' in x or
        'Avg' in x or
        'Shape' in x) and not 'Odd' in x and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    deps, score = rate_selected_features(
        df, deps, statName, home_team, away_team)
    best_deps = sorted(deps, reverse=True, key=lambda x: rate(df,
                                                              [x], statName, home_team, away_team)[0])[:deps_length]
    num_of_deps = len(best_deps[:int(accuracy)])

    for i in range(num_of_deps):
        set_of_stats = [list(x) for x in (combinations(best_deps, i+1))]
        for index, combination in enumerate(set_of_stats):
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
        accuracy = 2

        predicted_groups = predict_task(
            home_team, away_team, accuracy, match['_id'])

        for predicted_group in predicted_groups:
            match_coeff['homeTeam'] = match['homeTeam']
            match_coeff['awayTeam'] = match['awayTeam']
            db_predicted['homeTeam'] = home_team['name']
            db_predicted['userId'] = int(match['userId'])
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
            print(f'Predicting odd <{odd["name"]}>')

            predicted, accuracy_resulted, deps = predictAll(
                df, pairs, home_team, away_team, odd['name'], accuracy)
            parts.append(predicted)
            accuracies.append(accuracy_resulted)
            hints.append(odd['name'])
            full_deps = [*full_deps, *list(dict.fromkeys(deps))]
        for odd in group['items']:
            print(f'\nPredicting relative odd <{odd["name"]}>')

            relative_rate = rate(df, full_deps,
                                 odd['name'], home_team, away_team)[0]
            relative_odds.append(predict(df, pairs, home_team, away_team, odd['name'],
                                         list(set(full_deps)), relative_rate, False, None, None))
            relative_rates.append(relative_rate)
        all_group_results.append({
            'betName': group['name'],
            'hints': hints,
            'avg_accuracy': statistics.mean(accuracies),
            'rates': accuracies,
            'absolute_odds': parts,
            # 'alt_relative_odds': expand_odds(parts),
            # 'alt_relative_rates': accuracies,
            'relative_rates': relative_rates,
            'relative_odds': expand_odds(relative_odds),
        })
    return all_group_results


print("Redis Subscriber")

redis_conn = redis.Redis(
    host='host.docker.internal',
    port=6379,
    decode_responses=True)


def sub():
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("task")
    for message in pubsub.listen():
        if message.get("type") == "message":
            data = json.loads(message.get("data"))
            find_task(data['task_id'])


if __name__ == "__main__":
    sub()
