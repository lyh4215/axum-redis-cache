default:
    just --list


# for test
test:
    docker image prune -f
    docker compose up -d
    cargo test