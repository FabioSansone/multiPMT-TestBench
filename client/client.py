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
import progFEB
import argparse
from types import SimpleNamespace
import socket
import struct
import fcntl

#Generic Constants
MAX_RETRIES = 5

#ZMQ Constants
POLLER_TIMEOUT_CONNECTION = 20000 #in ms

#########################################
# Logging
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
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ip = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(), 0x8919, struct.pack('256s', interface[:15].encode()))[20:24])
        netmask = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(), 0x891b, struct.pack('256s', interface[:15].encode()))[20:24])
        
        ip_parts = list(map(int, ip.split('.')))
        mask_parts = list(map(int, netmask.split('.')))
        broadcast_parts = [ip_parts[i] | (~mask_parts[i] & 0xFF) for i in range(4)]
        return ".".join(map(str, broadcast_parts))
    except (OSError, IOError) as e:
        logger.warning(f"Impossibile ottenere broadcast per {interface}: {e}")
        return "255.255.255.255"


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



class Client:
    def __init__(self, context, server_ip, rc, hv, port=8001):
        self.context = context
        self.rc = rc
        self.hv = hv
        self.port = port
        self.client = None
        self.server_ip = server_ip
        self.client_id = b"Client"

    def send_json(self, data):
        try:
            self.client.send(json.dumps(data).encode("utf-8"))
        except Exception as e:
            logger.error(f"Something unexpected happened when sending data: {e}")

    def receive_json(self):
        try:
            return json.loads(self.client.recv().decode("utf-8"))
        except json.JSONDecodeError:
            logger.error("Error decoding JSON message")
        except Exception as e:
            logger.error(f"Unexpected error receiving data: {e}")
        return None
        
    def start_connection(self):
        try:
            
            if self.client:
                self.client.close()
                
            server_address = f"tcp://{self.server_ip}:{self.port}"
            self.client = self.context.socket(zmq.DEALER)
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
        attempt_hand = 0
        
        poller_hand = zmq.Poller()
        poller_hand.register(self.client, zmq.POLLIN)
        
        while not connected and (attempt_hand <= MAX_RETRIES):
            try:
                self.client.send(b"Ping")
                logger.info(f"Ping signal sent (attempt {attempt_hand + 1})")
                
                events_hand = dict(poller_hand.poll(POLLER_TIMEOUT_CONNECTION))
                
                if not events_hand:
                    attempt_hand += 1
                    logger.warning(f"No response from server, retry {attempt_hand}/{MAX_RETRIES}")
                    time.sleep(1)
                    continue
                
                if self.client in events_hand:
                    message = self.client.recv()
                    
                    if message == b"Alive":
                        logger.info("Server responded. Connection established")
                        self.client.send(b"Connection successful")
                        
                        events_ev = dict(poller_hand.poll(POLLER_TIMEOUT_CONNECTION))
                        if self.client in events_ev:
                            evproducer = self.client.recv()
                            if evproducer == b"EV":
                                try:
                                    self.rc.write(1, 127)
                                    time.sleep(0.1)
                                    self.rc.write(0, 127)
                                    time.sleep(0.1)
                                    self.rc.write(10, 65)
                                    time.sleep(0.1)
                                    self.rc.write(19, 0)
                                    time.sleep(0.1)
                                    self.rc.write(15, 0) 
                                    time.sleep(0.1)
                                    self.rc.write(16, 0)
                                    time.sleep(0.1)
                                except Exception as e:
                                    logger.error(f"RC setup failed: {e}")
                                    return False
                                
                                try:
                                    exec_command = ["./evproducer.sh", self.server_ip]
                                    process = subprocess.Popen(exec_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                                    logger.info("Evproducer started successfully")
                                except Exception as e:
                                    logger.error(f"Failed to start evproducer: {e}")
                                    return False
                                    
                                self.client.send(b"EV Success")

                                hv_setting = self.receive_json()
                                try:
                                    if hv_setting == 0:
                                        logger.info("Setting the HV Configuration and powering on the system")
                                        self.hv.setInitConfiguration(channels="all", voltage_set=1200, threshold_set=100, limit_trip_time=2, limit_voltage=100, limit_current=5, limit_temperature=50, rate_up=25, rate_down=25)
                                        self.hv.power_on(channels="all")
                                        self.client.send(b"HV Success")
                                    else:
                                        logger.info("Test configuration: skipping HV setup")
                                        self.client.send(b"Test Success")
                                
                                except Exception as e:
                                    logger.error(f"HV setup failed: {e}")
                                    return False  
                                
                                connected = True
                                return True    

                            else:
                                logger.warning(f"Unexpected message instead of EV: {evproducer}")
                                attempt_hand += 1
                                continue
                    
                    else:
                        logger.warning(f"Unexpected handshake message: {message}")
                        attempt_hand += 1
                        continue
                    
            except zmq.ZMQError as e:
                logger.critical(f"ZMQ Error during handshake: {e}")
            except Exception as e:
                logger.critical(f"Unexpected error during handshake: {e}")

        
        logger.error("Handshake failed after maximum retries")
        return False
    
    
    
    
    def handle_commands(self):

        poller = zmq.Poller()
        poller.register(self.client, zmq.POLLIN)


        while True:
            try:
                logger.info("Waiting for server command")
                events = dict(poller.poll())
                if not events:
                    continue
                
                if self.client in events:
                    server_command = self.receive_json()
                    logger.info(f"Received the following command {server_command}")
                    if server_command is None or not server_command:
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
                            if self.rc.write(addr, value):
                                write_t = {"response": "rc_write", "result": f"Successfully wrote the value {value} in register {addr}"}
                                self.send_json(write_t)
                                logger.info(f"Successfully wrote the value {value} in register {addr}")
                            else:
                                write_f = {"response": "rc_write", "result": f"It was not possible to write the value {value} in register {addr}"}
                                self.send_json(write_f)
                                logger.info(f"It was not possible to write the value {value} in register {addr}")
                        
                        if command == "read_address":
                            addr = server_command.get("address")
                            if self.rc.read(addr):
                                read_t = {"response": "rc_read", "result": f"{self.rc.read(addr)}"}
                                self.send_json(read_t)
                                logger.info(f"Successfully read the register {addr}")
                            else:
                                read_f = {"response": "rc_read", "result": f"{self.rc.read(addr)}"}
                                self.send_json(read_f)
                                logger.info(f"It was not possible to read the register {addr}")
                        
                        if command == "rc_monitoring":
                            regs = server_command.get("regs")
                            monitoring = {"response": "rc_mon", "result": self.rc.reg_monitoring(regs=regs)}
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
                            init_conf = {"response": "hv_init_conf", "result": self.hv.setInitConfiguration(channels=channel, voltage_set=voltage_set, threshold_set=threshold_set, 
                                                                                                       limit_trip_time=limit_trip_time, limit_voltage=limit_voltage, limit_current=limit_current, 
                                                                                                       limit_temperature=limit_temperature, rate_up=rate_up, rate_down=rate_down) }
                            self.send_json(init_conf)

                        if command == "set_voltage":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            voltage_set = server_command.get("voltage_set")    
                            v_set = {"response": "hv_voltage_set", "result": self.hv.set_voltage(channels=channel, voltage_set=voltage_set)}
                            self.send_json(v_set)
                        
                        if command == "set_threshold":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            threshold_set = server_command.get("threshold_set")
                            t_set = {"response": "hv_threshold_set", "result": self.hv.set_threshold(channels=channel, threshold_set=threshold_set)}
                            self.send_json(t_set)

                        if command == "set_power_on":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            set_power_on = {"response": "hv_power_on", "result": self.hv.power_on(channels=channel)}
                            self.send_json(set_power_on)

                        if command == "set_power_off":
                            port = server_command.get("port")
                            channel = server_command.get("channel")
                            set_power_off = {"response": "hv_power_off", "result" : self.hv.power_off(channels=channel)}
                            self.send_json(set_power_off)

                        
                        if command == "hv_calibration":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            set_hv_calib = {"response" : "hv_calibration", "result" : self.hv.channelsCalib(channels=channel)}
                            self.send_json(set_hv_calib)

                        if command == "hv_serial":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            set_hv_serial = {"response" : "hv_serial", "result": self.hv.getSerial(channels=channel)}
                            self.send_json(set_hv_serial)
                        
                        if command == "set_serial":
                            channels = server_command.get("channels")
                            port = server_command.get("port")
                            serials = server_command.get("serials")
                            set_pmt_serial = {"response" : "set_serial", "result": self.hv.setAllPMTSerial(channels=channels,serials=serials)}
                            self.send_json(set_pmt_serial)

                        if command == "hv_prog_feb":
                            channel = server_command.get("channels")
                            port = server_command.get("port")
                            baud = server_command.get("baud")
                            firmware = server_command.get("firmware")
                            set_start_up = {"response": "hv_start_up", "result": progFEB.main(channels=channel, port=port, baud=baud, firmware=firmware, rc=rc, hv=hv)}
                            self.send_json(set_start_up)
                           

            except zmq.ZMQError as e:
                logger.critical(f"ZMQ Error while handling commands: {e}")
                return False
            except Exception as e:
                logger.critical(f"Unexpected error in command handler: {e}")
                continue

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Client connection closed.")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--server_ip", action="store", type=str, help="The ip of the server")
    parser.add_argument("--port", action="store", type=int, help="The port of the connection with the server (default:8001)", default=8001)
    parser.add_argument("--hv_port", action="store", type=str, help="The serial port of the modbus FEB (default:/dev/ttyPS1)", default="/dev/ttyPS1")
    parser.add_argument("--hv_interface", action="store", type=str, help="How to connect modbus to the FEBs (default:tcp)", default="tcp")
    parser.add_argument("--interface", action="store", type=str, help="The network interface of the client (default=eth0)", default="eth0")

    args = parser.parse_args()

    server_ip = args.server_ip
    
    # Se non è fornito IP, provo a scoprirlo dinamicamente
    if not server_ip:
        attempt = 0
        while True:
            attempt += 1
                    
            broadcast_ip = get_broadcast_address(interface=args.interface)
            logger.info(f"[Attempt {attempt}] Trying to discover server on {broadcast_ip}")
            
            server_ip = discover_server_ip(broadcast_ip=broadcast_ip, port=args.port)
            
            if server_ip is None:
                if (attempt % 10 == 0):
                    logger.critical("Server still not found after 10 attempts. Check the server status.")
                else:  
                    logger.warning("Server discovery failed. Retrying in 2 seconds...")
                time.sleep(2)
                continue

            logger.info(f"Discovered server IP: {server_ip}")
            break
            
            

    if args.hv_interface == "tcp":
        params = SimpleNamespace(mode = 'tcp',
                            host = 'localhost',
                            port = 502)
    else:
        params = SimpleNamespace(mode = 'rtu',
                            host = 'localhost',
                            port = args.hv_port)
    
    

    context = zmq.Context()
    rc = RC()
    hv = HV(params=params)
    
    
    client = Client(context=context, server_ip=server_ip, rc=rc, hv=hv, port=args.port)

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
