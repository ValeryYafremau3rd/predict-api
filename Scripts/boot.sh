cd /home/ubuntu/fr2 &&
docker compose stop &&
docker system prune -a --volumes --force &&
COMPOSE_HTTP_TIMEOUT=200  docker-compose -f docker-compose.yml up -d
