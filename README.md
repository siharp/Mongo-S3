# Mongo-S3

# Build docker image
docker build -t test:1

# Run docker container
docker run --rm --network mynetwork --name pipeline --env-file .env test:1

