#!/bin/bash
set -e

# Install dependencies
if ! command -v socat &>/dev/null || ! command -v ss &>/dev/null || ! command -v java &>/dev/null; then
  sudo apt update && sudo apt install -y socat iproute2
fi

COOJA_DIR="/home/user/contiki-ng/tools/cooja"
SIM_FILE="/home/user/simulations/nsds-simulation.csc"

# Start Cooja in GUI mode
cd "$COOJA_DIR"
./gradlew run --args="$SIM_FILE" &
COOJA_PID=$!

# Wait for Cooja serial socket on port 60001
until ss -lptn 'sport = :60001' | grep -q 60001; do
  sleep 2
  kill -0 $COOJA_PID 2>/dev/null || { echo "Cooja process died."; exit 1; }
done

# Build and start tunslip6
cd /home/user/contiki-ng/tools/serial-io
make tunslip6
sudo ./tunslip6 -a localhost -p 60001 fd00::1/64 &
TUNSLIP_PID=$!

# Wait for tun0 interface
until ip link show tun0 &>/dev/null; do
  sleep 1
  kill -0 $TUNSLIP_PID 2>/dev/null || { echo "tunslip6 process died."; exit 1; }
done

# Resolve and test MQTT
MQTT_IP=$(getent hosts $MQTT_BROKER_HOST | awk '{print $1}')
[ -z "$MQTT_IP" ] && { echo "ERROR: Could not resolve mqtt hostname"; exit 1; }

# Start socat MQTT proxy (IPv6 fd00::1:1883 -> IPv4 MQTT broker)
sudo socat TCP6-LISTEN:1883,fork,bind=[fd00::1] TCP4:${MQTT_BROKER_HOST:-mqtt}:${MQTT_BROKER_PORT:-1883} &
SOCAT_PID=$!

echo "Running: Cooja ($COOJA_PID), Tunslip6 ($TUNSLIP_PID), Socat ($SOCAT_PID)"
wait $COOJA_PID
