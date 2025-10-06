
import requests

# env
API = 'https://v3.football.api-sports.io'
KEY = "36def15c52268beb41f99b47d610e473"


# 39 pl
# 78 bl
# 135 sa
# 140 p
# 61 l1
available_leagues = [39, 78, 135, 140, 61]

def get_data(uri):
    return requests.get(API + uri,
             headers={"x-rapidapi-key": KEY}).json()
