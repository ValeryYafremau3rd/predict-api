
import services.mongodb as db

group_by_league = {
    "$group": {
        "_id": "$league",
    }
}
add_league = {
    "$addFields": {
        'league': '$league.name',
        'season': '$league.season'
    }
}

pipeline = [
    add_league,
    group_by_league
]

teams_pipeline = [
    add_league
]


def get_teams(league, season):
    return db.fixtures.aggregate([
        add_league, {
            "$match": {
                "league": league,
                "season": season,
            }
        }, {
            "$addFields": {
                'homeTeam': '$teams.home.name',
            }
        },
        {
            "$group": {
                "_id": {
                    'id': "$teams.home.id",
                    'name': '$teams.home.name'
                }
            }
        }, {'$sort': {'teams.name': 1}}
    ])


def get_leagues():
    return db.fixtures.aggregate(pipeline)
