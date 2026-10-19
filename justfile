# Print the operator guide. This is the default recipe.
[default]
help:
    @echo 'Surface operator guide'
    @echo ''
    @echo 'just'
    @echo '    Same text as just help.'
    @echo 'just help'
    @echo '    This guide.'
    @echo 'just --list'
    @echo '    Short index of the same recipes.'
    @echo 'just --help'
    @echo '    just'\''s own flag help, not this guide.'
    @echo ''
    @echo 'just server-dev'
    @echo '    Environment: dev.'
    @echo '    Directory: deployments/.'
    @echo '    Starts: docker compose up, using deployments/docker-compose.yml only.'
    @echo '    That stack is the Connector, the cache, and the dev Vault.'
    @echo '    The dev token, the dev encryption key, and the dev Vault root'
    @echo '    token are for this local stack only.'
    @echo '    This stack allows 240 requests per source per minute so two'
    @echo '    Surfaces on one machine can poll, connect, and accept.'
    @echo '    Refuses: a second compose file, and it does not start a client.'
    @echo ''
    @echo 'just server-stage'
    @echo '    Environment: stage.'
    @echo '    Directory: the repository root.'
    @echo '    Requires: VAULT_ADDR and VAULT_TOKEN in the environment.'
    @echo '    If either is missing, empty, or whitespace, the recipe exits'
    @echo '    before a container starts and names the variable. It does not'
    @echo '    print the value.'
    @echo '    Starts: the Connector image from deployments/Dockerfile.connector.'
    @echo '    Publishes 8080 and 8081. Punch listens on :8081.'
    @echo '    Does not start Vault dev mode. Does not use the local-stack token,'
    @echo '    encryption key, or Vault root token. Does not create a git branch.'
    @echo '    If server-dev already holds 8080 or 8081, this start fails and'
    @echo '    leaves that stack running.'
    @echo ''
    @echo 'just server-production'
    @echo '    Environment: production.'
    @echo '    Same inputs, image, ports, and refusals as just server-stage.'
    @echo ''
    @echo 'just surface <key> [listen]'
    @echo '    Environment: dev client only. Not stage and not production.'
    @echo '    Directory: the repository root.'
    @echo '    Starts: go run ./cmd/surface against http://127.0.0.1:8080.'
    @echo '    key is required. listen defaults to 127.0.0.1:9001.'
    @echo '    Uses the documented local-stack connector token.'
    @echo '    Does not build or execute a surface binary.'
    @echo '    A terminal opens a keyboard UI. A pipe keeps the line commands.'
    @echo ''
    @echo 'When invocation, compose, health, readiness, local-stack credentials,'
    @echo 'punch, relay, or these recipes change, update this guide, the README'
    @echo 'run section, and deployments/README.md in the same change.'

# Start the local Connector, cache, and dev Vault.
server-dev:
    cd "{{justfile_directory()}}/deployments" && docker compose up

# Start the stage Connector. Requires VAULT_ADDR and VAULT_TOKEN.
server-stage:
    @just --quiet server-image stage

# Start the production Connector. Requires VAULT_ADDR and VAULT_TOKEN.
server-production:
    @just --quiet server-image production

# Dev client from source. key is required. listen defaults to 127.0.0.1:9001.
surface key listen='127.0.0.1:9001':
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{justfile_directory()}}"
    if [ -z {{quote(key)}} ]; then
        echo "surface: key is required" >&2
        exit 1
    fi
    export SURFACE_CONNECTOR_TOKEN="${SURFACE_CONNECTOR_TOKEN:-dev}"
    exec go run ./cmd/surface http://127.0.0.1:8080 {{quote(key)}} {{quote(listen)}}

[private]
server-image env:
    #!/usr/bin/env bash
    set -euo pipefail
    name="{{env}}"
    missing=0
    addr="${VAULT_ADDR-}"
    token="${VAULT_TOKEN-}"
    addr="${addr#"${addr%%[![:space:]]*}"}"
    addr="${addr%"${addr##*[![:space:]]}"}"
    token="${token#"${token%%[![:space:]]*}"}"
    token="${token%"${token##*[![:space:]]}"}"
    if [ -z "$addr" ]; then
        printf '%s: VAULT_ADDR is required\n' "$name" >&2
        missing=1
    fi
    if [ -z "$token" ]; then
        printf '%s: VAULT_TOKEN is required\n' "$name" >&2
        missing=1
    fi
    if [ "$missing" -ne 0 ]; then
        exit 1
    fi
    root="{{justfile_directory()}}"
    docker build -f "$root/deployments/Dockerfile.connector" -t "surface-connector:$name" "$root"
    docker run --rm --name "surface-connector-$name" \
        -p 8080:8080 -p 8081:8081 \
        -e VAULT_ADDR \
        -e VAULT_TOKEN \
        -e SURFACE_PUNCH_ADDR=:8081 \
        "surface-connector:$name"
