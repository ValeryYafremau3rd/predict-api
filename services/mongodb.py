
import pymongo

# move to env
CONNECT_STRING = 'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'

client = pymongo.MongoClient(CONNECT_STRING)
statistics = client["statistics"]
custom_odds = statistics["custom-odds"]
groups = statistics["groups"]
matches = statistics["matches"]
fixtures = statistics["fixtures"]
strategy = statistics["predicts"]
queue = statistics["queue"]
