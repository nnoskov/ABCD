#!/usr/bin/env bash
set -Eeuo pipefail

LOG_DIR="/home/alku/alku/ABCD/.cache/abcd"
LOG_FILE="${LOG_DIR}/kiosk.log"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*"; }

# все выводы — в лог + в stdout (удобно и для journalctl)
exec > >(tee -a "$LOG_FILE") 2>&1

trap 'rc=$?; log "ERR rc=$rc line=$LINENO cmd=$BASH_COMMAND"; exit $rc' ERR
trap 'log "EXIT rc=$?";' EXIT

URL="http://127.0.0.1:8000/api/workplace"
SETTINGS_URL="http://127.0.0.1:8000/api/settings"

log "kiosk.sh start; URL=$URL"
log "uid=$(id -u) user=$(id -un) DISPLAY=${DISPLAY:-<empty>} XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-<empty>} DBUS_SESSION_BUS_ADDRESS=${DBUS_SESSION_BUS_ADDRESS:-<empty>}"

# wait for X (systemd user service can start before DISPLAY is exported)
if [ -z "${DISPLAY:-}" ]; then
  for i in {1..60}; do
    if [ -S /tmp/.X11-unix/X0 ]; then
      export DISPLAY=:0
      log "set DISPLAY=:0 (X socket found)"
      break
    fi
    sleep 1
  done
fi

if [ -z "${DISPLAY:-}" ]; then
  log "FATAL: DISPLAY is still empty; no X session"
  exit 14
fi

if [ "$(id -u)" -eq 0 ]; then
  log "FATAL: kiosk.sh is running as root. Must run as the autologin user (e.g. postamat) inside GUI session."
  exit 13
fi

has_curl=0
if command -v curl >/dev/null 2>&1; then
  has_curl=1
else
  log "WARN: curl not found"
fi

# ждать поднятия web
for i in {1..120}; do
  if [ "$has_curl" -eq 1 ]; then
    if curl -fsS "$SETTINGS_URL" >/dev/null 2>&1; then
      log "web ready (settings ok) on attempt=$i"
      break
    fi
  else
    # fallback без curl: попробуем через /dev/tcp (bash)
    if (exec 3<>/dev/tcp/127.0.0.1/8000) >/dev/null 2>&1; then
      exec 3>&- 3<&-
      log "web port 8000 open on attempt=$i"
      break
    fi
  fi
  if [ "$i" -eq 120 ]; then
    log "WARN: web not ready after 120s, will still try to launch browser"
  fi
  sleep 1
done

# выключить засыпание/блокировку экрана (GNOME)
log "disable idle/screen lock (best effort)"
if [ -n "${DISPLAY:-}" ] && command -v gsettings >/dev/null 2>&1; then
  gsettings set org.gnome.desktop.session idle-delay 0 || true
  gsettings set org.gnome.desktop.screensaver lock-enabled false || true
else
  log "WARN: no DISPLAY or gsettings; skip screen lock tweaks"
fi

# chromium command (ubuntu variants)
CHROME="$(command -v chromium-browser || command -v chromium || true)"
if [ -z "$CHROME" ]; then
  exit 1
  log "FATAL: chromium not found in PATH"
  exit 127
fi

# snap chromium can write only inside ~/snap/chromium/*
if [ -d "${HOME}/snap/chromium" ]; then
  KIOSK_PROFILE="${HOME}/snap/chromium/common/kiosk-profile"
else
  KIOSK_PROFILE="${HOME}/.cache/chromium-kiosk"
fi
rm -rf "$KIOSK_PROFILE" >/dev/null 2>&1 || true
mkdir -p "$KIOSK_PROFILE"
chmod 700 "$KIOSK_PROFILE" || true

# если chromium уже запущен (например, восстановился после ребута) — флаги могут не примениться
pkill -f "chromium.*--user-data-dir=${KIOSK_PROFILE}" >/dev/null 2>&1 || true
pkill -x chromium-browser >/dev/null 2>&1 || true
pkill -x chromium >/dev/null 2>&1 || true
sleep 1

log "launch: $CHROME (profile=$KIOSK_PROFILE)"
exec "$CHROME" \
  --kiosk \
  --start-fullscreen \
  --ozone-platform=x11 \
  --noerrdialogs \
  --no-first-run \
  --no-default-browser-check \
  --disable-infobars \
  --disable-session-crashed-bubble \
  --disable-features=TranslateUI \
  --overscroll-history-navigation=0 \
  --disable-pinch \
  --force-device-scale-factor=1 \
  --incognito \
  --disk-cache-dir=/tmp/chrome-cache \
  --user-data-dir="$KIOSK_PROFILE" \
  --window-position=0,0 \
  --window-size=1024,768 \
  "$URL"
