IMAGE=${DOCKER_IMAGE:-grosinosky/rust-subscriber}
cargo build --release
docker build . -t $IMAGE
docker push $IMAGE

