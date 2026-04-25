#!/usr/bin/python3
import argparse
import socket
import time
import threading
import queue
import re
import paho.mqtt.client as mqtt

# Global queue for commands to ClearOne
cmd_queue = queue.Queue()
# Track last known gate values to only publish on change
gate_state_cache = {}

class ClearOneClient:
    def __init__(self, host, user, password, verbose=False):
        self.host = host
        self.user = user
        self.password = password
        self.telnet_port = 23
        self.verbose = verbose
        self.sock = None
        self.connected = False
        self.send_lock = threading.Lock()
        self.greport_units = None

    def log(self, msg):
        if self.verbose:
            print(f"[ClearOne] {msg}")

    def connect(self, greport_units=None):
        if greport_units is not None:
            self.greport_units = greport_units
        try:
            self.sock = socket.socket()
            self.sock.settimeout(5)
            self.sock.connect((self.host, self.telnet_port))
            self.connected = True
            self.log("Telnet connected")
            return self.authenticate()
        except Exception as e:
            self.log(f"Connection failed: {e}")
            self.connected = False
            return False

    def authenticate(self):
        try:
            # Wait for 'user' prompt
            while b'user' not in self.sock.recv(512):
                pass
            self.sock.send((self.user + "\r").encode())

            # Wait for 'pass' prompt
            while b'pass' not in self.sock.recv(512):
                pass
            self.sock.send((self.password + "\r").encode())

            # Wait for Auth confirmation
            while True:
                resp = self.sock.recv(512)
                if b'Authenticated' in resp:
                    self.log("Authenticated")
                    time.sleep(0.5)
                    if self.greport_units:
                        for unit in self.greport_units:
                            cmd = f"#{unit} GREPORT 1\r"
                            self.sock.send(cmd.encode())
                            self.log(f"GREPORT enabled for unit {unit}")
                            time.sleep(0.1)
                    return True
                if b'Invalid' in resp:
                    self.log("Invalid credentials")
                    return False
        except Exception as e:
            self.log(f"Authentication error: {e}")
            return False

    def send_command(self, cmd):
        if not self.connected:
            self.log("Not connected, cannot send command")
            return
        try:
            full_cmd = cmd.strip() + "\r"
            with self.send_lock:
                self.sock.send(full_cmd.encode())
            self.log(f"Sent: {full_cmd.strip()}")
        except Exception as e:
            self.log(f"Send failed: {e}")
            self.connected = False

    def recv_data(self):
        try:
            data = self.sock.recv(512).decode('utf-8')
            return data
        except socket.timeout:
            return ""
        except Exception as e:
            self.log(f"Receive failed: {e}")
            self.connected = False
            return ""

    def close(self):
        try:
            self.sock.close()
        except:
            pass
        self.connected = False


# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected with result code {rc}")
    # Subscribe to all ClearOne set commands with group
    client.subscribe("clearone/+/+/+/+/set")
    # Subscribe to ClearOne set commands without group
    client.subscribe("clearone/+/+/+/set")


def on_message(client, userdata, msg):
    topic_parts = msg.topic.split('/')

    # Check basic structure and that it starts with 'clearone' and ends with 'set'
    if topic_parts[0] != 'clearone' or topic_parts[-1] != 'set':
        return

    dev = topic_parts[1]
    command = topic_parts[2].upper()
    value = msg.payload.decode('utf-8').strip()

    # Determine if topic has a group or not
    if len(topic_parts) == 6:
        # Format: clearone/{DEV}/{COMMAND}/{GROUP}/{CHANNEL}/set
        channel = topic_parts[3].upper()
        group = topic_parts[4].upper()
        clearone_cmd = f"#{dev} {command} {channel} {group} {value}"
    elif len(topic_parts) == 5:
        # Format: clearone/{DEV}/{COMMAND}/{CHANNEL}/set
        group = None
        channel = topic_parts[3].upper()
        clearone_cmd = f"#{dev} {command} {channel} {value}"
    else:
        # Invalid topic format
        return

    # Put command in queue
    cmd_queue.put(clearone_cmd)


# Thread to process outgoing commands
def process_commands(clearone):
    while True:
        cmd = cmd_queue.get()
        if not clearone.connected:
            clearone.connect()
        if clearone.connected:
            clearone.send_command(cmd)
        time.sleep(0.1)  # Slight delay to avoid overwhelming the device


# Thread to listen for ClearOne responses
def listen_clearone(clearone, mqtt_client):
    while True:
        if not clearone.connected:
            clearone.log("Disconnected, attempting reconnect...")
            if clearone.connect():
                clearone.log("Reconnected successfully")
            else:
                time.sleep(5)
                continue
        data = clearone.recv_data()
        if data:
            if clearone.verbose:
                print(f"[CLEARONE] Received = {data}")
            lines = re.split(r'\r|\n', data)
            for line in lines:
                line = line.strip()
                if not line or not line.startswith('#'):
                    continue
                parts = line.split()

                # Handle GATE specially - format is #DEV GATE HEXVALUE
                # Parse bitmap and publish individual per-mic topics as 1 or 0
                if len(parts) == 3 and parts[1].upper() == 'GATE':
                    dev = parts[0][1:]
                    gate_hex = parts[2]
                    try:
                        gate_val = int(gate_hex, 16)
                        for mic in range(1, 9):
                            bit = (gate_val >> (mic - 1)) & 1
                            cache_key = f"{dev}/GATE/{mic}"
                            if gate_state_cache.get(cache_key) != bit:
                                gate_state_cache[cache_key] = bit
                                topic = f"clearone/{dev}/GATE/{mic}"
                                mqtt_client.publish(topic, str(bit))
                                if clearone.verbose:
                                    print(f"[MQTT] Published {topic} = {bit}")
                    except ValueError:
                        clearone.log(f"Could not parse GATE value: {gate_hex}")
                    continue

                # Original logic for everything else
                if len(parts) < 4:
                    continue
                dev = parts[0][1:]
                command = parts[1].upper()
                channel = parts[2].upper()
                if len(parts) >= 5:
                    group = parts[3].upper()
                    value = parts[4]
                    topic = f"clearone/{dev}/{command}/{channel}/{group}/state"
                else:
                    group = None
                    value = parts[3]
                    topic = f"clearone/{dev}/{command}/{channel}/state"

                mqtt_client.publish(topic, value)
                if clearone.verbose:
                    print(f"[MQTT] Published {topic} = {value}")


# Thread to keep the ClearOne telnet session alive
def clearone_keepalive(clearone):
    while True:
        time.sleep(60)
        if not clearone.connected:
            continue
        try:
            clearone.send_command("#** VER")
            if clearone.verbose:
                print("[ClearOne] Keepalive sent (#** VER)")
        except Exception as e:
            clearone.log(f"Keepalive failed: {e}")
            clearone.connected = False


def main():
    parser = argparse.ArgumentParser(description="Bidirectional ClearOne <-> MQTT bridge")
    parser.add_argument("--clearone-host", required=True, help="ClearOne IP/hostname")
    parser.add_argument("--clearone-user", required=True, help="ClearOne username")
    parser.add_argument("--clearone-pass", required=True, help="ClearOne password")
    parser.add_argument("--mqtt-host", required=True, help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--mqtt-user", help="MQTT username")
    parser.add_argument("--mqtt-pass", help="MQTT password")
    parser.add_argument("-g", "--greport", action="append", dest="greport_units",
                        metavar="UNIT_ID",
                        help="Enable GREPORT for unit ID (optional, can be specified multiple times e.g. -g 10 -g H2)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    clearone = ClearOneClient(args.clearone_host, args.clearone_user, args.clearone_pass, verbose=args.verbose)
    clearone.connect(greport_units=args.greport_units)

    mqtt_client = mqtt.Client()
    if args.mqtt_user:
        mqtt_client.username_pw_set(args.mqtt_user, args.mqtt_pass)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(args.mqtt_host, args.mqtt_port, 60)
    mqtt_client.loop_start()

    threading.Thread(target=process_commands, args=(clearone,), daemon=True).start()
    threading.Thread(target=listen_clearone, args=(clearone, mqtt_client), daemon=True).start()
    threading.Thread(target=clearone_keepalive, args=(clearone,), daemon=True).start()

    print("Bridge running. Press Ctrl+C to exit.")
    print(f"GREPORT enabled for units: {args.greport_units or 'none'}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        clearone.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()