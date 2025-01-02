import asyncio
from websockets.asyncio.client import connect
import pymongo
import json
import pandas

myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
fixtures = myclient["statistics"]["fixtures"]
predicted = myclient["statistics"]["predicts"]
queue = myclient["statistics"]["queue"]


accuracy = 7


async def hello():
    async with connect("ws://localhost:8357", ping_interval=None) as websocket:
        df = None
        matches = queue.find({'userId': 1})
        first_line = True
        for match in matches:
            #try:
                home_team = fixtures.find_one({'teams.home.name': match['homeTeam']})[
                    'teams']['home']
                away_team = fixtures.find_one({'teams.away.name': match['awayTeam']})[
                    'teams']['away']
                match_coeff = {}
                db_predicted = {}
                db_predicted['odds'] = {}
                await websocket.send(f"{home_team['id']}>{away_team['id']}>bets>{accuracy}>{match['_id']}")
                while True:
                    match_coeff['homeTeam'] = match['homeTeam']
                    match_coeff['awayTeam'] = match['awayTeam']
                    db_predicted['homeTeam'] = match['homeTeam']
                    db_predicted['awayTeam'] = match['awayTeam']
                    db_predicted['league'] = fixtures.find_one({'teams.home.id':home_team['id']})['league']['name']
                    message = json.loads(await websocket.recv())
                    print(message)
                    db_predicted['hints'] = message['hints']
                    db_predicted['odds'][message['betName']] = {
                        'relative': [
                            1 / x // 0.01 / 100 for x in message['relative_odds']],
                        'absolute': [
                            1 / x // 0.01 / 100 for x in message['absolute_odds']],
                        'rates': message['rates']
                    }
                    match_coeff[message['betName']] = [[
                        1 / x // 0.01 / 100 for x in message['relative_odds']], message['avg_accuracy']]
                    if message['done'] == True:
                        break
                predicted.insert_one(db_predicted)
                print(match_coeff)
                if first_line:
                    first_line = False
                    df = pandas.DataFrame([match_coeff])
                else:
                    df = df._append(match_coeff, ignore_index=True)
                df.to_excel("odds_snapshot.xlsx")
                queue.delete_one({'_id': match['_id']})
            #except:
            #    print(f'{match["homeTeam"]} : {match["awayTeam"]} failed to predict')

if __name__ == "__main__":
    asyncio.run(hello())

