#!/usr/bin/env bash
set -e
xset s off -dpms
openbox &
exec /home/alku/alku/ABCD/kiosk.sh
