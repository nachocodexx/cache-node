readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/cache-node
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
