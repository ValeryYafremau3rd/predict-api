

#env
PORT = 6379

import redis
r = redis.Redis(host='host.docker.internal', port=PORT, decode_responses=True)