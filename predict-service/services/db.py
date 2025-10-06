
import pymongo

# env var
#DB_CONNECT_LINE = 'mongodb://user:pass@host.docker.internal:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'
DB_CONNECT_LINE = 'mongodb://user:pass@192.168.0.109:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'

client = pymongo.MongoClient(DB_CONNECT_LINE)

db = client["statistics"]

fixtures = db["fixtures"]
predicted = db["predicts"]
queue = db["queue"]
groups = db["groups"]
custom_odds = db["custom-odds"]
matches = db["matches"]
teams = db["teams"]
leagues = db["leagues"]
