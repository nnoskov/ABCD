#!/usr/bin/env bash
set -Eeuo pipefail

# Postamats: local launcher without systemd.
#
# Commands:
#   ./run_stack_linux.sh start [ENV_PATH]
#   ./run_stack_linux.sh restart [ENV_PATH]
#   ./run_stack_linux.sh stop
#   ./run_stack_linux.sh status
#   ./run_stack_linux.sh install-shortcut
#
# Compatibility with the old launcher is preserved:
#   ./run_stack_linux.sh .env
# means "start .env".

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
PROJECT_DIR="$(dirname "$SCRIPT_PATH")"
SCRIPT_NAME="$(basename "$SCRIPT_PATH")"

if [[ ${EUID} -eq 0 ]]; then
    echo "ОШИБКА: не запускайте весь $SCRIPT_NAME через sudo."
    echo "Запускайте от обычного пользователя; sudo нужен только для первичной настройки QR-сканера."
    exit 1
fi

COMMAND="${1:-start}"
ENV_PATH=".env"
case "$COMMAND" in
    start|restart)
        [[ $# -ge 2 ]] && ENV_PATH="$2"
        ;;
    stop|status|install-shortcut|help|-h|--help)
        ;;
    *)
        ENV_PATH="$COMMAND"
        COMMAND="start"
        ;;
esac

cd "$PROJECT_DIR"

RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}/postamats-stack-${UID}"
mkdir -p "$RUNTIME_DIR"
chmod 700 "$RUNTIME_DIR" 2>/dev/null || true

WEB_PID_FILE="$RUNTIME_DIR/web.pid"
DAEMON_PID_FILE="$RUNTIME_DIR/daemon.pid"
BROWSER_PID_FILE="$RUNTIME_DIR/firefox.pid"

WEB_PID=""
DAEMON_PID=""
BROWSER_PID=""

QR_SERIAL_DEVICE="${QR_SERIAL_DEVICE:-/dev/ttyACM0}"
QR_SERIAL_GROUP="${QR_SERIAL_GROUP:-dialout}"

read_pid() {
    local file="$1"
    [[ -r "$file" ]] && tr -cd '0-9' < "$file" || true
}

pid_alive() {
    local pid="${1:-}"
    [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

pid_matches() {
    local pid="${1:-}"
    local needle="$2"
    [[ -r "/proc/$pid/cmdline" ]] || return 1
    tr '\0' ' ' < "/proc/$pid/cmdline" | grep -Fq -- "$needle"
}

remove_pid_if_same() {
    local file="$1"
    local expected="$2"
    [[ "$(read_pid "$file")" == "$expected" ]] && rm -f "$file"
}

stop_one() {
    local file="$1"
    local needle="$2"
    local label="$3"
    local pid
    pid="$(read_pid "$file")"

    if ! pid_alive "$pid" || ! pid_matches "$pid" "$needle"; then
        rm -f "$file"
        return 0
    fi

    echo "Останавливаю $label (PID $pid)..."
    kill -TERM "$pid" 2>/dev/null || true

    for _ in {1..50}; do
        pid_alive "$pid" || break
        sleep 0.1
    done

    if pid_alive "$pid"; then
        echo "$label не завершился по SIGTERM; отправляю SIGKILL."
        kill -KILL "$pid" 2>/dev/null || true
    fi

    rm -f "$file"
}

stop_stack() {
    # Daemon is stopped first so production control logic cannot continue while
    # the web process is being replaced.
    stop_one "$DAEMON_PID_FILE" "app.daemon.main" "daemon"
    stop_one "$WEB_PID_FILE" "uvicorn app.web.main:app" "web"
    stop_one "$BROWSER_PID_FILE" "firefox" "Firefox"
    echo "Стек остановлен."
}

load_environment() {
    if [[ ! -f "$PROJECT_DIR/.venv/bin/activate" ]]; then
        echo "ОШИБКА: не найдено виртуальное окружение $PROJECT_DIR/.venv/bin/activate"
        exit 1
    fi

    # shellcheck disable=SC1091
    source "$PROJECT_DIR/.venv/bin/activate"

    [[ "$ENV_PATH" = /* ]] || ENV_PATH="$PROJECT_DIR/$ENV_PATH"
    if [[ ! -f "$ENV_PATH" ]]; then
        echo "ОШИБКА: не найден файл окружения $ENV_PATH"
        exit 1
    fi

    set -a
    # shellcheck disable=SC1090
    source "$ENV_PATH"
    set +a

    WEB_HOST="${WEB_HOST:-127.0.0.1}"
    WEB_PORT="${WEB_PORT:-8000}"
    QR_SERIAL_DEVICE="${QR_SERIAL_DEVICE:-/dev/ttyACM0}"
    QR_SERIAL_GROUP="${QR_SERIAL_GROUP:-dialout}"

    case "$WEB_HOST" in
        0.0.0.0|::|"[::]") BROWSER_HOST="127.0.0.1" ;;
        *) BROWSER_HOST="$WEB_HOST" ;;
    esac
    WORKPLACE_URL="http://${BROWSER_HOST}:${WEB_PORT}/api/workplace"
}

ensure_scanner_access() {
    if ! getent group "$QR_SERIAL_GROUP" >/dev/null 2>&1; then
        echo "ПРЕДУПРЕЖДЕНИЕ: группа $QR_SERIAL_GROUP отсутствует. QR-сканер автоматически не настраиваю."
        return 0
    fi

    # Permanent solution: membership in dialout. This requires sudo only once.
    if ! id -nG "$USER" | tr ' ' '\n' | grep -Fxq "$QR_SERIAL_GROUP"; then
        echo "Добавляю пользователя $USER в группу $QR_SERIAL_GROUP..."
        sudo usermod -aG "$QR_SERIAL_GROUP" "$USER"
        echo "Группа настроена. После следующего входа в Ubuntu доступ к COM-устройствам будет постоянным."
    fi

    if [[ ! -e "$QR_SERIAL_DEVICE" ]]; then
        echo "ПРЕДУПРЕЖДЕНИЕ: QR-сканер не найден: $QR_SERIAL_DEVICE"
        echo "Web и daemon всё равно будут запущены."
        return 0
    fi

    if [[ -r "$QR_SERIAL_DEVICE" ]]; then
        echo "QR-сканер доступен для чтения: $QR_SERIAL_DEVICE"
        return 0
    fi

    # The newly added supplementary group is not active in the already running
    # desktop session until the next login. For this first session grant only
    # read access; do not use chmod 666.
    if command -v setfacl >/dev/null 2>&1; then
        echo "Временно выдаю пользователю $USER персональное право только на чтение $QR_SERIAL_DEVICE..."
        sudo setfacl -m "u:${USER}:r" "$QR_SERIAL_DEVICE"
    else
        echo "setfacl не найден; временно добавляю только read-доступ к $QR_SERIAL_DEVICE."
        sudo chmod o+r "$QR_SERIAL_DEVICE"
    fi

    if [[ ! -r "$QR_SERIAL_DEVICE" ]]; then
        echo "ОШИБКА: не удалось получить право чтения $QR_SERIAL_DEVICE"
        ls -l "$QR_SERIAL_DEVICE" || true
        exit 1
    fi

    echo "QR-сканер доступен для чтения: $QR_SERIAL_DEVICE"
    echo "После следующего выхода/входа в Ubuntu временное право больше не потребуется."
}

wait_for_web() {
    python - "$BROWSER_HOST" "$WEB_PORT" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
deadline = time.monotonic() + 15.0

while time.monotonic() < deadline:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            raise SystemExit(0)
    except OSError:
        time.sleep(0.2)

raise SystemExit(1)
PY
}

start_firefox() {
    local firefox_bin="${FIREFOX_BIN:-firefox}"
    # Non-hidden path works cleanly with Ubuntu's Snap build of Firefox.
    local firefox_profile="${FIREFOX_PROFILE_DIR:-$HOME/PostamatsFirefoxProfile}"

    [[ "${POSTAMATS_OPEN_BROWSER:-1}" == "0" ]] && return 0

    if ! command -v "$firefox_bin" >/dev/null 2>&1; then
        echo "ПРЕДУПРЕЖДЕНИЕ: Firefox не найден; web и daemon продолжают работать."
        return 0
    fi

    if [[ -z "${DISPLAY:-}" && -z "${WAYLAND_DISPLAY:-}" ]]; then
        echo "ПРЕДУПРЕЖДЕНИЕ: нет графической сессии; Firefox не запускаю."
        return 0
    fi

    mkdir -p "$firefox_profile"
    echo "Открываю Firefox в полноэкранном режиме: $WORKPLACE_URL"
    "$firefox_bin" --kiosk -no-remote -profile "$firefox_profile" "$WORKPLACE_URL" >/dev/null 2>&1 &
    BROWSER_PID=$!
    echo "$BROWSER_PID" > "$BROWSER_PID_FILE"
}

cleanup() {
    local rc=$?
    set +e

    [[ -n "$DAEMON_PID" ]] && kill -TERM "$DAEMON_PID" 2>/dev/null || true
    [[ -n "$WEB_PID" ]] && kill -TERM "$WEB_PID" 2>/dev/null || true
    [[ -n "$BROWSER_PID" ]] && kill -TERM "$BROWSER_PID" 2>/dev/null || true

    [[ -n "$DAEMON_PID" ]] && wait "$DAEMON_PID" 2>/dev/null || true
    [[ -n "$WEB_PID" ]] && wait "$WEB_PID" 2>/dev/null || true
    [[ -n "$BROWSER_PID" ]] && wait "$BROWSER_PID" 2>/dev/null || true

    [[ -n "$DAEMON_PID" ]] && remove_pid_if_same "$DAEMON_PID_FILE" "$DAEMON_PID"
    [[ -n "$WEB_PID" ]] && remove_pid_if_same "$WEB_PID_FILE" "$WEB_PID"
    [[ -n "$BROWSER_PID" ]] && remove_pid_if_same "$BROWSER_PID_FILE" "$BROWSER_PID"

    exit "$rc"
}

start_stack() {
    local old_web old_daemon
    old_web="$(read_pid "$WEB_PID_FILE")"
    old_daemon="$(read_pid "$DAEMON_PID_FILE")"

    if { pid_alive "$old_web" && pid_matches "$old_web" "uvicorn app.web.main:app"; } || \
       { pid_alive "$old_daemon" && pid_matches "$old_daemon" "app.daemon.main"; }; then
        echo "Стек уже запущен. Для безопасного перезапуска используйте: ./$SCRIPT_NAME restart"
        return 0
    fi

    load_environment
    ensure_scanner_access

    trap cleanup EXIT
    trap 'exit 0' INT TERM HUP

    echo "Запускаю web..."
    python -m uvicorn app.web.main:app --host "$WEB_HOST" --port "$WEB_PORT" --workers 1 &
    WEB_PID=$!
    echo "$WEB_PID" > "$WEB_PID_FILE"

    echo "Запускаю daemon..."
    python -m app.daemon.main &
    DAEMON_PID=$!
    echo "$DAEMON_PID" > "$DAEMON_PID_FILE"

    echo
    echo "WORKPLACE: $WORKPLACE_URL"
    echo "EVENTS:    http://${BROWSER_HOST}:${WEB_PORT}/api/events"
    echo "WEB PID:   $WEB_PID"
    echo "DAEMON PID: $DAEMON_PID"
    echo "CTRL+C — корректно остановить стек"
    echo

    if wait_for_web; then
        start_firefox
    else
        echo "ПРЕДУПРЕЖДЕНИЕ: web не открыл порт за 15 секунд; Firefox не запускаю."
    fi

    set +e
    wait -n "$WEB_PID" "$DAEMON_PID"
    local rc=$?
    set -e

    echo "Один из процессов web/daemon завершился. Останавливаю оставшуюся часть стека."
    return "$rc"
}

show_status() {
    local web daemon browser
    web="$(read_pid "$WEB_PID_FILE")"
    daemon="$(read_pid "$DAEMON_PID_FILE")"
    browser="$(read_pid "$BROWSER_PID_FILE")"

    if pid_alive "$web" && pid_matches "$web" "uvicorn app.web.main:app"; then
        echo "WEB:     RUNNING (PID $web)"
    else
        echo "WEB:     STOPPED"
    fi

    if pid_alive "$daemon" && pid_matches "$daemon" "app.daemon.main"; then
        echo "DAEMON:  RUNNING (PID $daemon)"
    else
        echo "DAEMON:  STOPPED"
    fi

    if pid_alive "$browser" && pid_matches "$browser" "firefox"; then
        echo "FIREFOX: RUNNING (PID $browser)"
    else
        echo "FIREFOX: STOPPED"
    fi

    if [[ -e "$QR_SERIAL_DEVICE" ]]; then
        [[ -r "$QR_SERIAL_DEVICE" ]] && echo "QR:      READABLE ($QR_SERIAL_DEVICE)" || echo "QR:      NO READ ACCESS ($QR_SERIAL_DEVICE)"
        ls -l "$QR_SERIAL_DEVICE" || true
    else
        echo "QR:      NOT CONNECTED ($QR_SERIAL_DEVICE)"
    fi
}

install_shortcut() {
    local desktop_dir launcher

    if command -v xdg-user-dir >/dev/null 2>&1; then
        desktop_dir="$(xdg-user-dir DESKTOP)"
    else
        desktop_dir="$HOME/Desktop"
    fi
    [[ -n "$desktop_dir" ]] || desktop_dir="$HOME/Desktop"

    mkdir -p "$desktop_dir"
    launcher="$desktop_dir/АЛКУ.desktop"
    chmod +x "$SCRIPT_PATH"

    cat > "$launcher" <<EOF_DESKTOP
[Desktop Entry]
Version=1.0
Type=Application
Name=АЛКУ
Comment=Запуск или безопасный перезапуск web и daemon
Exec="$SCRIPT_PATH" restart
Path=$PROJECT_DIR
Terminal=true
Icon=system-run
Categories=Utility;
StartupNotify=false
EOF_DESKTOP

    chmod +x "$launcher"
    command -v gio >/dev/null 2>&1 && gio set "$launcher" metadata::trusted true >/dev/null 2>&1 || true

    echo "Ярлык создан: $launcher"
    echo "Первый клик запускает стек; следующий клик безопасно перезапускает web и daemon без reboot ОС."
}

print_help() {
    cat <<EOF_HELP
Использование:
  ./$SCRIPT_NAME start [ENV_PATH]
  ./$SCRIPT_NAME restart [ENV_PATH]
  ./$SCRIPT_NAME stop
  ./$SCRIPT_NAME status
  ./$SCRIPT_NAME install-shortcut

По умолчанию используется .env из каталога проекта.
EOF_HELP
}

case "$COMMAND" in
    start) start_stack ;;
    restart) stop_stack; start_stack ;;
    stop) stop_stack ;;
    status) show_status ;;
    install-shortcut) install_shortcut ;;
    help|-h|--help) print_help ;;
    *) print_help; exit 2 ;;
esac
