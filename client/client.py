#!/usr/bin/env python3
#coding=utf-8
import zmq
import logging
import time
import json
import subprocess
from rc_client import RC
from hv_client import HV
from mon_conf import MON
import prog_FEB
import argparse

import socket
import struct
import fcntl

#########################################
logger = logging.getLogger("Client")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
client_error_handler = logging.FileHandler('client_error.log')
client_error_handler.setLevel(logging.INFO)
client_error_handler.setFormatter(formatter)
logger.addHandler(client_error_handler)
#########################################


#########################################
###BROADCAST UDP###
#########################################

def get_broadcast_address(interface='eth0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ip = socket.inet_ntoa(fcntl.ioctl(
        s.fileno(), 0x8919, struct.pack('256s', interface[:15].encode()))[20:24])
    netmask = socket.inet_ntoa(fcntl.ioctl(
        s.fileno(), 0x891b, struct.pack('256s', interface[:15].encode()))[20:24])
    
    ip_parts = list(map(int, ip.split('.')))
    mask_parts = list(map(int, netmask.split('.')))
    broadcast_parts = [ip_parts[i] | (~mask_parts[i] & 0xFF) for i in range(4)]
    return ".".join(map(str, broadcast_parts))


def discover_server_ip(broadcast_ip, port, timeout=3):
    MESSAGE = b"DISCOVER_SERVER"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(timeout)
        s.sendto(MESSAGE, (broadcast_ip, port))
        try:
            data, addr = s.recvfrom(1024)
            if data == b"I am server":
                return addr[0]
        except socket.timeout:
            return None


##############################################

PING_INTERVAL = 6 # s
POLLER_COMMANDS_TIMEOUT = 100 # ms

context = zmq.Context()
rc = RC()
hv = HV()
mon = MON()

class Client:
    def __init__(self, server_ip, port=8001, hv_port="/dev/ttyPS1"):
        self.port = port
        self.hv_port = hv_port
        self.client = None
        self.server_ip = server_ip
        self.client_id = b"Client"

    def send_json(self, data):
        try:
            return self.client.send(json.dumps(data).encode("utf-8"))
        except Exception as e:
            logger.error(f"Something unexpected happened when sending data: {e}")

    def receive_json(self):
        try:
            return json.loads(self.client.recv().decode("utf-8"))
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
                        exec_command = ["/root/client/evproducer.sh", self.server_ip]
                        logger.info(f"Executing evproducer with: {exec_command}")
                        process = subprocess.Popen(exec_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        logger.info("Evproducer has started successfully")
                        self.client.send(b"EV Success")

                        hv_setting = self.receive_json()
                        if hv_setting == 0:
                            logger.info("Setting the HV Configuration and powering on the system")
                            hv.set_hv_init_configuration(channels="all", port="/dev/ttyPS1", voltage_set=1200, threshold_set=100, limit_trip_time=2, limit_voltage=100, limit_current=5, limit_temperature=50, rate_up=25, rate_down=25)
                            hv.power_on(channels="all", port="/dev/ttyPS1")
                            self.client.send(b"HV Success")
                            connected = True
                            return True
                        else:
                            logger.info("This is a test configuration for the FEB. No need to set the HV and power up the system")
                            self.client.send(b"Test Success")
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
                        
                        if command == "rc_monitoring":
                            regs = server_command.get("regs")
                            monitoring = {"response": "rc_mon", "result": rc.reg_monitoring(regs=regs)}
                            self.send_json(monitoring)
                    
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
                        
                        if command == "set_threshold":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            threshold_set = server_command.get("threshold_set")
                            t_set = {"response": "hv_threshold_set", "result": hv.set_threshold(channel, threshold_set, port)}
                            self.send_json(t_set)

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

                        if command == "hv_serial":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            set_hv_serial = {"response" : "hv_serial", "result": hv.get_serial(channels=channel, port=port)}
                            self.send_json(set_hv_serial)

                        if command == "hv_prog_feb":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            baud = server_command.get("baud")
                            firmware = server_command.get("firmware")
                            set_start_up = {"response": "hv_start_up", "result": prog_FEB.main(channels=channel, port=port, baud=baud, firmware=firmware)}
                            self.send_json(set_start_up)
                    

                    elif cmd_type == "mon_command":
                        command = server_command.get("command")
                        if command == "monitoring":
                            rc_flag = 0
                            hv_flag = 0
                            mon_flag = 0
                            result = []

                            rc_flag, hv_flag, mon_flag = int(server_command.get("rc_flag")), int(server_command.get("hv_flag")), int(server_command.get("mon_flag"))
                            
                            if rc_flag == 1:
                                try:
                                    rc_mon = rc.reg_monitoring([20,21,22,23,24,25,26])
                                    result.append(rc_mon)
                                except:
                                    pass
                            
                            if hv_flag == 1:
                                try:
                                    hv_mon = hv.read_volt(channels=[1,2,3,4,5,6,7], port="/dev/ttyPS1")
                                    result.append(hv_mon)
                                except:
                                    pass
                            
                            if mon_flag == 1:
                                try:
                                    mon_mon = mon.read_mon_data()
                                    result.append(mon_mon)
                                except:
                                    pass
                            

                            set_monitoring_all = {"response": "monitoring", "result": result}
                            self.send_json(set_monitoring_all)


                            
                

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

    parser = argparse.ArgumentParser()
    parser.add_argument("--server_ip", action="store", type=str, help="The ip of the server")
    parser.add_argument("--port", action="store", type=int, help="The port of the connection with the server (default:8001)", default=8001)
    parser.add_argument("--hv_port", action="store", type=str, help="The serial port of the modbus FEB (default:/dev/ttyPS1)", default="/dev/ttyPS1")
    parser.add_argument("--interface", action="store", type=str, help="The network interface of the client (default=eth0)", default="eth0")

    args = parser.parse_args()

    server_ip = args.server_ip
    if not server_ip:
        # Se non è fornito IP, provo a scoprirlo dinamicamente
        broadcast_ip = get_broadcast_address(interface=args.interface)
        logger.info(f"Trying to discover server IP using broadcast on {broadcast_ip}")
        server_ip = discover_server_ip(broadcast_ip=broadcast_ip, port=args.port)
        if server_ip is None:
            logger.critical("Server discovery failed. Exiting.")
            exit(1)
        else:
            logger.info(f"Discovered server IP: {server_ip}")


    client = Client(server_ip=args.server_ip, port=args.port, hv_port=args.hv_port)

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
