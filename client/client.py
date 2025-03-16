#!/usr/bin/env python3
#coding=utf-8
import zmq
import logging
import time
import json
import subprocess
import multiprocessing as mp
from rc_client import RC
from hv_client import HV

#########################################
logger = logging.getLogger("Client")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
client_error_handler = logging.FileHandler('client_error.log')
client_error_handler.setLevel(logging.INFO)
client_error_handler.setFormatter(formatter)
logger.addHandler(client_error_handler)
#########################################

PING_INTERVAL = 6 # s
POLLER_COMMANDS_TIMEOUT = 100 # ms

context = zmq.Context()
rc = RC()
hv = HV()


class Client:
    def __init__(self, port=8001, hv_port="/dev/ttyPS1"):
        self.port = port
        self.hv_port = hv_port
        self.client = None
        self.server_ip = "172.16.24.107"
        self.client_id = b"Client"

    def send_json(self, data):
        try:
            return self.client.send(json.dumps(data).encode("utf-8"))
        except Exception as e:
            logger.error(f"Something unexpected happened when sending data: {e}")

    def receive_json(self):
        try:
            return json.loads(self.client.recv())
        except json.JSONDecodeError:
            logger.error("Error decoding JSON message")
        except Exception as e:
            logger.error(f"Unexpected error receiving data: {e}")

    def start_connection(self):
        try:
            server_address = f"tcp://{self.server_ip}:{self.port}"
            self.client = context.socket(zmq.DEALER)
            self.client.setsockopt(zmq.IDENTITY, self.client_id)
            self.client.connect(server_address)
            logger.info(f"Client started on port {self.port} and connected to {server_address}")
        except zmq.ZMQError as e:
            logger.critical(f"Failed to connect client on the server address {server_address}: {e}")
            self.client = None

    def handshake(self):
        if self.client is None:
            return False
        connected = False
        last_ping = time.time()
        while not connected:
            try:
                if (time.time() - last_ping) >= PING_INTERVAL:
                    self.client.send(b"Ping")
                    logger.info("Ping signal sent")
                    last_ping = time.time()
                else:
                    time.sleep(PING_INTERVAL)
                    logger.info("No response from server. Reconnecting...")
                    continue
                message = self.client.recv()
                if message == b"Alive":
                    logger.info("Server responded. Connection established")
                    self.client.send(b"Connection successful")
                    evproducer = self.client.recv()
                    if evproducer == b"EV":
                        rc.write(1, 127)
                        time.sleep(0.1)
                        rc.write(0, 127)
                        time.sleep(0.1)
                        rc.write(10, 65)
                        time.sleep(0.1)
                        rc.write(19, 0)
                        time.sleep(0.1)
                        rc.write(15, 0)
                        time.sleep(0.1)
                        rc.write(16, 0)
                        time.sleep(0.1)
                        hv.set_hv_init_configuration(channels="all", port="/dev/ttyPS1", voltage_set=1200, threshold_set=100, limit_trip_time=2, limit_voltage=100, limit_current=5, limit_temperature=50, rate_up=25, rate_down=25)
                        hv.power_on(channels="all", port="/dev/ttyPS1")
                        exec_command = ["/root/evproducer.sh"]
                        logger.info(f"Executing evproducer with: {exec_command}")
                        process = subprocess.Popen(exec_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        logger.info("Evproducer has started successfully")
                        self.client.send(b"EV Success")
                        connected = True
                        return True
            except zmq.ZMQError as e:
                logger.critical(f"ZMQ Error during handshake: {e}")
            except Exception as e:
                logger.critical(f"Unexpected error during handshake: {e}")

    def handle_commands(self):

        poller = zmq.Poller()
        poller.register(self.client, zmq.POLLIN)


        while True:
            try:
                logger.info("Waiting for server command")
                events = dict(poller.poll())
                if self.client in events:
                    server_command = self.receive_json()
                    logger.info(f"Received the following command {server_command}")
                    if server_command is None:
                        logger.error("Failed to receive valid command from server.")
                        continue
                    
                    cmd_type = server_command.get("type")
                    if cmd_type == "client_command":
                        command = server_command.get("command")
                        logger.info(f"Executing command: {command}")
                        if command == "exit":
                            logger.info("Stopping Evproducer")
                            result = subprocess.run(["killall", "evproducer"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                            if result.returncode != 0:
                                error_msg = result.stderr.decode().strip()
                                logger.error(f"Error executing killall: {error_msg}")
                            else:
                                logger.info("Acquisition process terminated via killall")

                            logger.info("Exit command received. Returning to handshake state.")
                            return False
                    
                    elif cmd_type == "rc_command":
                        command = server_command.get("command")
                        logger.info(f"Executing command: {command}")
                        if command == "write_address":
                            value = server_command.get("value")
                            addr = server_command.get("address")
                            if rc.write(server_command.get("address"), server_command.get("value")):
                                write_t = {"response": "rc_write", "result": f"Successfully wrote the value {value} in register {addr}"}
                                self.send_json(write_t)
                                logger.info(f"Successfully wrote the value {value} in register {addr}")
                            else:
                                write_f = {"response": "rc_write", "result": f"It was not possible to write the value {value} in register {addr}"}
                                self.send_json(write_f)
                                logger.info(f"It was not possible to write the value {value} in register {addr}")
                    
                    elif cmd_type == "hv_command":
                        command = server_command.get("command")

                        if command == "set_init_configuration":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            voltage_set = server_command.get("voltage_set")    
                            threshold_set = server_command.get("threshold_set")
                            limit_trip_time = server_command.get("limit_trip_time")    
                            limit_voltage = server_command.get("limit_voltage")
                            limit_current = server_command.get("limit_current")
                            limit_temperature = server_command.get("limit_temperature")
                            rate_up = server_command.get("rate_up")
                            rate_down = server_command.get("rate_down")
                            init_conf = {"response": "hv_init_conf", "result": hv.set_hv_init_configuration(port, channel, voltage_set, threshold_set, limit_trip_time, limit_voltage, limit_current, limit_temperature, rate_up, rate_down)}
                            self.send_json(init_conf)

                        if command == "set_voltage":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            voltage_set = server_command.get("voltage_set")    
                            v_set = {"response": "hv_voltage_set", "result": hv.set_voltage(channel, voltage_set, port)}
                            self.send_json(v_set)

                        if command == "set_power_on":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            set_power_on = {"response": "hv_power_on", "result": hv.power_on(channel, port)}
                            self.send_json(set_power_on)

                        if command == "set_power_off":
                            port = server_command.get("port")
                            channel = server_command.get("channel")


                            set_power_off = {

                                "response": "hv_power_off",
                                "result" : hv.power_off(channel, port)

                            }

                            self.send_json(set_power_off)

                        
                        if command == "hv_calibration":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            set_hv_calib = {"response" : "hv_calibration", "result" : hv.channels_calib(channels=channel, port=port)}
                            self.send_json(set_hv_calib)


                            
                

            except zmq.ZMQError as e:
                logger.critical(f"ZMQ Error while handling commands: {e}")
                return False
            except Exception as e:
                logger.critical(f"Unexpected error in command handler: {e}")
                return False

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Client connection closed.")

if __name__ == "__main__":
    client = Client()
    try:
        while True:
            client.start_connection()
            if not client.handshake():
                logger.error("Handshake failed. Retrying...")
                continue
            if not client.handle_commands():
                logger.info("Returning to handshake state...")
    except KeyboardInterrupt:
        logger.info("Client interrupted. Exiting...")
    finally:
        client.close()
        context.term()
