cd /home/ubuntu/fr2 &&
docker system prune -a --volumes --force &&
COMPOSE_HTTP_TIMEOUT=200  docker-compose -f docker-compose.yml up -d