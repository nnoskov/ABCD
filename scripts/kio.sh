#!/usr/bin/env bash
set -e

URL="http://127.0.0.1:8000/api/workplace"

# ждать поднятия web
for i in {1..90}; do
  if curl -fsS "$URL" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# выключить засыпание/блокировку экрана (GNOME)
gsettings set org.gnome.desktop.session idle-delay 0 || true
gsettings set org.gnome.desktop.screensaver lock-enabled false || true

# chromium command (ubuntu variants)
CHROME="$(command -v chromium-browser || command -v chromium || true)"
if [ -z "$CHROME" ]; then
  exit 1
fi

exec "$CHROME" \
  --kiosk \
  --noerrdialogs \
  --disable-infobars \
  --disable-session-crashed-bubble \
  --disable-features=TranslateUI \
  --overscroll-history-navigation=0 \
  "$URL"
