#!/usr/bin/env bash
# Isolated Kilo 7.8.8 + wasit-hostd sub-agent probe. See README.md in this directory.
#
#   run.sh env            create $E2E (dirs, isolated HOME/XDG, kilo.jsonc, fixture files)
#   run.sh fake           run the scripted provider in the foreground (needs no PID file)
#   run.sh hostd          run `wasit-hostd serve` in the foreground with the isolated env
#   run.sh drive <args>   run drive.py (socket client) with the isolated env
#
# Nothing here touches the owner's Kilo config, ~/.marsad, or the repos under /work.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E="${E2E:-/tmp/kilo/e2e}"
PORT="${PORT:-18090}"
KILO_BIN_DIR="${KILO_BIN_DIR:-/home/mewais/.kilo/bin}"
HOSTD_BIN="${HOSTD_BIN:-/work/mewais/Marsad/Wasit/target/debug/wasit-hostd}"

export HOME="$E2E/home"
export XDG_CONFIG_HOME="$E2E/home/.config"
export XDG_DATA_HOME="$E2E/home/.local/share"
export XDG_CACHE_HOME="$E2E/home/.cache"
export XDG_STATE_HOME="$E2E/home/.local/state"
export KILO_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true
export KILO_DISABLE_AUTOUPDATE=1
# kilo is found through PATH by the driver (`kilo-bridge -- kilo acp`).
export PATH="$KILO_BIN_DIR:/usr/local/bin:/usr/bin:/bin"

cmd="${1:-}"; shift || true

case "$cmd" in
  env)
    mkdir -p "$E2E/home" "$XDG_CONFIG_HOME/kilo" "$XDG_DATA_HOME" "$XDG_CACHE_HOME" \
             "$XDG_STATE_HOME" "$E2E/data" "$E2E/work"
    printf 'probe fixture\n' > "$E2E/work/hello.txt"
    printf 'second file\n' > "$E2E/work/notes.txt"
    cat > "$XDG_CONFIG_HOME/kilo/kilo.jsonc" <<EOF
{
  "\$schema": "https://app.kilo.ai/config.json",
  "autoupdate": false,
  "model": "fake/fake-model",
  "provider": {
    "fake": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Fake",
      "options": { "baseURL": "http://127.0.0.1:$PORT/v1", "apiKey": "fake" },
      "models": {
        "fake-model": {
          "name": "Fake Model",
          "tool_call": true,
          "limit": { "context": 200000, "output": 8192 }
        }
      }
    }
  },
  "permission": { "*": "allow" }
}
EOF
    echo "env ready: E2E=$E2E port=$PORT"
    ;;
  fake)
    exec python3 "$HERE/fake_openai_server.py" --port "$PORT" \
      --release "$E2E/release" --release-parent "$E2E/release_parent" \
      --hold-parent-flag "$E2E/hold_parent_ack" --log "$E2E/requests.jsonl" --workdir "$E2E/work"
    ;;
  hostd)
    exec "$HOSTD_BIN" serve --data "$E2E/data"
    ;;
  drive)
    exec python3 "$HERE/drive.py" "$@"
    ;;
  *)
    echo "usage: run.sh env|fake|hostd|drive <args>" >&2
    exit 2
    ;;
esac
