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

class ClearOneClient:
    def __init__(self, host, user, password, verbose=False):
        self.host = host
        self.user = user
        self.password = password
        self.telnet_port = 23
        self.verbose = verbose
        self.sock = None
        self.connected = False

    def log(self, msg):
        if self.verbose:
            print(f"[ClearOne] {msg}")

    def connect(self):
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
        time.sleep(0.05)  # Slight delay to avoid overwhelming the device

# Thread to listen for ClearOne responses
def listen_clearone(clearone, mqtt_client):
    while True:
        if not clearone.connected:
            time.sleep(1)
            continue
        data = clearone.recv_data()
        if data:
            lines = re.split(r'\r|\n', data)
            for line in lines:
                line = line.strip()
                if not line or not line.startswith('#'):
                    continue
                # Parse command line
                parts = line.split()
                if len(parts) < 4:
                    continue  # Not enough parts to parse
                dev = parts[0][1:]  # Remove #
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
    parser = argparse.ArgumentParser(description="Bidirectional ClearOne ↔ MQTT bridge")
    parser.add_argument("--clearone-host", required=True, help="ClearOne IP/hostname")
    parser.add_argument("--clearone-user", required=True, help="ClearOne username")
    parser.add_argument("--clearone-pass", required=True, help="ClearOne password")
    parser.add_argument("--mqtt-host", required=True, help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--mqtt-user", help="MQTT username")
    parser.add_argument("--mqtt-pass", help="MQTT password")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    clearone = ClearOneClient(args.clearone_host, args.clearone_user, args.clearone_pass, verbose=args.verbose)
    clearone.connect()

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
