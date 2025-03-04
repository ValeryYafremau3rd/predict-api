import traceback
import redis
import json
import pymongo
import stat_calculator as sc
import predictor as pd
import pandas
from sklearn.preprocessing import StandardScaler
from itertools import combinations
from multiprocessing.pool import Pool
import asyncio
import dataset as ds
import statistics
from bson.objectid import ObjectId

myclient = pymongo.MongoClient(
    'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
matches = myclient["statistics"]["matches"]
fixtures = myclient["statistics"]["fixtures"]
relations = myclient["statistics"]["relations"]
queue = myclient["statistics"]["queue"]
predicted = myclient["statistics"]["predicts"]
all_fixtures = fixtures.find()

homeTeamStatNames = []
awayTeamStatNames = []
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

    # for statName in pairs[0]:
    #    if 'Home Team' in statName and not 'Name' in statName and not 'Avg' in statName:
    #        homeTeamStatNames.append(statName)
    #    elif 'Away Team' in statName and not 'Name' in statName and not 'Avg' in statName:
    #        awayTeamStatNames.append(statName)

    # sc.recent_encounter(pairs)

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


def predict(df, pairs, home_team, away_team, stat_to_predict, deps, rate, is_test, scaler, regr):
    data = []
    # recent_encounter = df.loc[(df['homeTeamId'] == home_team_name) &
    #                          (df['awayTeamId'] == away_team_name)].iloc[0]
    df = df.fillna(0)
    homeTeam = df.loc[(df['homeTeamId'] == home_team)].iloc[0]
    awayTeam = df.loc[(df['awayTeamId'] == away_team)].iloc[0]
    newdf = df.loc[(df['homeTeamId'] == home_team) ^
                   (df['awayTeamId'] == away_team)]

#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
#        newdf['awayTeamId'] != away_team) & (newdf[stat_to_predict] == 1)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
#        newdf['awayTeamId'] != away_team) & (newdf[stat_to_predict] == 0)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
#        newdf['awayTeamId'] == away_team) & (newdf[stat_to_predict] == 1)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
#        newdf['awayTeamId'] == away_team) & (newdf[stat_to_predict] == 0)].iloc[-1]
#    newdf.drop_duplicates(keep='last')

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
        # elif 'Recent' in dep:
        #    data.append(recent_encounter[dep.replace('Recent Encounter ', '')])

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

#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
#        newdf['awayTeamId'] != away_team) & (newdf[calc_stat] == 1)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
#        newdf['awayTeamId'] != away_team) & (newdf[calc_stat] == 0)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
#        newdf['awayTeamId'] == away_team) & (newdf[calc_stat] == 1)].iloc[-1]
#    newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
#        newdf['awayTeamId'] == away_team) & (newdf[calc_stat] == 0)].iloc[-1]
#    newdf.drop_duplicates(keep='last')

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


def predictProbs(df, pairs, home_team, away_team, statName, accuracy=6):
    deps = [x for x in avg_stat_names if (
        'Avg' in x or
        'Shape' in x) and not 'Odd' in x and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    deps = select_logistic_features(
        df, deps, statName, home_team, away_team)

    result = rate(df, deps, statName, home_team, away_team)

    predicted = predict(df, pairs, home_team, away_team, statName,
                        deps, result[0], False, result[1], result[2])
    return (predicted, result[0], deps)


def predictPairs(df, pairs, home_team, away_team, statName, accuracy=6):
    results = []
    resultNames = []
    best_pair = []
    best_pair_result = 0
    deps_length = 15  # int(accuracy) * 3
    deps = [x for x in avg_stat_names if (
        'Avg' in x or
        'Shape' in x) and not 'Odd' in x and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]
    deps, score = rate_selected_features(
        df, deps, statName, home_team, away_team)
    best_deps = sorted(deps, reverse=True, key=lambda x: rate(df,
                                                              [x], statName, home_team, away_team)[0])
    best_home = [x for x in best_deps if 'Home Team ' in x][:10]
    best_away = [x for x in best_deps if 'Away Team ' in x][:10]
    print('best pair')
    for i in best_home:
        for j in best_away:
            rated = rate(df, [i, j], statName, home_team, away_team)
            if rated[0] > best_pair_result:
                best_pair_result = rated[0]
                best_pair = [i, j]
    best_deps = [x for x in best_deps if not x in best_pair][:deps_length]
    num_of_deps = len(best_deps[:int(accuracy)])

    # print('rest')
    # print(num_of_deps, len(best_deps))
    # for i in range(num_of_deps):
    #    set_of_stats = [list(x) for x in (combinations(best_deps, i+1))]
    #    for index, combination in enumerate(set_of_stats):
    #        combined = [*best_pair, *combination]
    #        resultNames.append(combined)
    #        results.append(
    #            rate(df, combined, statName, home_team, away_team))

    # index_max = [x[0] for x in results].index(max([x[0] for x in results]))
    predicted = predict(df, pairs, home_team, away_team, statName,
                        best_pair, best_pair_result // 0.01 / 100, False, None, None)
    return (predicted, best_pair_result // 0.01 / 100, best_pair)


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


def avoid_zero_value(val, zero=0.01):
    return zero if val == 0 else val


def max_value(val, max=0.99):
    return max if val >= 1 else val


def expand_odds(odds):
    coeff = 1 / sum(odds)
    return [x * coeff for x in odds]


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

            #newdf = df.loc[(df['homeTeamId'] == home_team) ^
            #               (df['awayTeamId'] == away_team)].fillna(0)
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
            #    newdf['awayTeamId'] != away_team) & (newdf[odd['name']] == 1)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
            #    newdf['awayTeamId'] != away_team) & (newdf[odd['name']] == 0)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
            #    newdf['awayTeamId'] == away_team) & (newdf[odd['name']] == 1)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
            #    newdf['awayTeamId'] == away_team) & (newdf[odd['name']] == 0)].iloc[-1]
            #newdf.drop_duplicates(keep='last')

            predicted, accuracy_resulted, deps = predictAll(
                df, pairs, home_team, away_team, odd['name'], accuracy)
            parts.append(predicted)
            # predicted, accuracy_resulted, deps = predictProbs(
            #    df, pairs, home_team, away_team, odd['name'], accuracy)
            # parts.append(min_value(max_value(predicted)))
            accuracies.append(accuracy_resulted)
            hints.append(odd['name'])
            full_deps = [*full_deps, *list(dict.fromkeys(deps))]
        for odd in group['items']:
            print(f'\nPredicting relative odd <{odd["name"]}>')

            #newdf = df.loc[(df['homeTeamId'] == home_team) ^
            #               (df['awayTeamId'] == away_team)].fillna(0)
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
            #    newdf['awayTeamId'] != away_team) & (newdf[odd['name']] == 1)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] == home_team) & (
            #    newdf['awayTeamId'] != away_team) & (newdf[odd['name']] == 0)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
            #    newdf['awayTeamId'] == away_team) & (newdf[odd['name']] == 1)].iloc[-1]
            # newdf.loc[-1] = newdf[(newdf['homeTeamId'] != home_team) & (
            #    newdf['awayTeamId'] == away_team) & (newdf[odd['name']] == 0)].iloc[-1]
            #newdf.drop_duplicates(keep='last')

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


def start_saved_tasks():
    for task in queue.find({}, projection={"_id": 1}):
        redis_conn.publish('task', json.dumps({'task_id': str(task['_id'])}))


if __name__ == "__main__":
    sub()
    start_saved_tasks()
