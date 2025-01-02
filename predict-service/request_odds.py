import http.client
import json
import time
import pymongo
import requests
import pandas

# pl
88637
# bl
96463
# ll
127733
# sa
110163
# l1
12821


myclient = pymongo.MongoClient('mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')

mydb = myclient["bets"]
odds = mydb["odds"]
leagues = mydb["leagues"]
matches = mydb["matches"]


conn = http.client.HTTPSConnection("1xbet-api.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "d10e6ce29fmsh09e1bc0327a672bp17d348jsna90f9161fcc7",
    'x-rapidapi-host': "1xbet-api.p.rapidapi.com"
}

# conn.request("GET", "/matches?sport_id=1&league_id=88637&mode=line", headers=headers)
#
# res = conn.getresponse()
# data = res.read()
#
# matches.insert_many(json.loads(data.decode("utf-8",'surrogateescape'))['data'])

match_id = matches.find_one()['id']
#conn.request("GET", f"/matches/{match_id}/markets?mode=line", headers=headers)
#
#res = conn.getresponse()
#data = res.read()
##print(data.decode().replace('\\udcbfap', 'cap'))
#
#odds.insert_one({'matchId': match_id, 'odds': json.loads(
#    data.decode().replace('\\udcbfap', 'cap'))['data']})
df = None
odds_to_track = ['1x2', 'Scores In Each Half', 'Team 1 Scores In Halves', 'Team 2 Scores In Halves']
for i, odd in enumerate(odds.find()):
    line = {}
    match = matches.find_one({'id': odd['matchId']})
    line['homeTeam'] = match['home_team']
    line['awayTeam'] = match['away_team']
    for item in odd['odds'].items():
        if item[1]['name'] in odds_to_track:
            line[item[1]['name']] = [x['odds'] for x in item[1]['outcomes']]
    if i == 0:
        df = pandas.DataFrame([line])
    else:
        df = df._append(line, ignore_index=True)
    df.to_excel("bets.xlsx")

