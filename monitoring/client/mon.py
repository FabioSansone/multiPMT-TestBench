import zmq
import logging
import time
import json
import datetime
import argparse
import sys
import mmap

from bme280 import BME280
from tla2024 import TLA2024

IIC_BUS = 1
CHIP_ADDRESS_BME = 0x76
CHIP_ADDRESS_TLA = 0x49
REGS = [x for x in range(20, 27)]
MAX_REG_ADDR = 50

bme = BME280(IIC_BUS, CHIP_ADDRESS_BME)
tla = TLA2024(IIC_BUS, CHIP_ADDRESS_TLA)


#########################################
logger = logging.getLogger("Monitoring")
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

#Error File Handler
mon_error_handler = logging.FileHandler('/var/log/monitoring.log')
mon_error_handler.setLevel(logging.WARNING)
mon_error_handler.setFormatter(formatter)

logger.addHandler(mon_error_handler)
#########################################


PING_INTERVAL = 6 


class Client:

    def __init__(self, port, server_ip, client_id):
         
        self.port = port
        self.context = zmq.Context()
        self.client = None
        self.ip = server_ip
        self.client_id = client_id.encode("utf-8")

        try:
            self.fid = open('/dev/uio0', 'r+b', 0)
        except:
            logger.error("E: UIO device /dev/uio0 not found")
            sys.exit(-1)
        

        try:
            self.regs = mmap.mmap(self.fid.fileno(), 0x10000)
        except Exception as e:
            logger.error(f"E: Failed to map memory: {e}")
            self.fid.close()
            sys.exit(-1)




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




    
    def start_connection(self):
        try:
            server_address = f"tcp://{self.ip}:{self.port}"
            self.client = self.context.socket(zmq.DEALER)
            self.client.setsockopt(zmq.IDENTITY, self.client_id)


            self.client.connect(server_address)
            logger.info(f"Client started on port {self.port} and connected to the address {server_address}")
        
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
                    last_ping = time.time()
                
                else:
                    time.sleep(PING_INTERVAL)
                    logger.info("No response from server. Reconnecting...")
                    continue

                message = self.client.recv()
                if message == b"Alive":
                    logger.info("Server responded. Connection established")
                    self.client.send(b"Connection successful")
                    connected = True
                    return True

            except zmq.ZMQError as e:
                logger.critical(f"ZMQ Error during handshake: {e}")

            except Exception as e:
                logger.critical(f"Unexpected error during handshake: {e}")



    def close_connection(self):
        if self.client:
            self.client.close()
        self.context.term()





    def handle_server(self, freq, timer):

        poller = zmq.Poller()
        poller.register(self.client, zmq.POLLIN)

        if timer is not None:
            if timer < freq:
                logger.warning(f"The frequency value, {freq}, is greater than the timer value {timer}. It is not possible to acquire data. Timer has been changed to frequency value plus 10 seconds")
                timer = freq + 20

        start_time = time.time()
                    
        while not timer or (time.time() - start_time <= timer):
            start_freq = time.time()
            events = dict(poller.poll(timeout=100))
            if self.client in events and self.client.recv() == b"Stop":
                logger.info("Clients have been halted by the server")
                self.close_connection()
                return None
            
            self.read_mon_data()
            self.read_rates()

            delta = freq - (time.time() - start_freq)
            time.sleep(max(delta, 0.01))


    
    def send_data(self):
        try:
            config = self.receive_json()
            if not self.validate_config(config):
                return "Reconnect"
            
            freq = int(config["freq"])
            timer = int(config["timer"]) if config["timer"] else None
    

            self.handle_server(freq, timer)

        except KeyboardInterrupt:
            logger.info("Client interrupted")
            self.client.send(b"StopC")
            self.close_connection()
            return "Reconnect"

        except zmq.ZMQError as e:
            logger.error(f"Connection error: {e}")
            return None

        except Exception as e:
            logger.critical(f"Unexpected error: {e}")
            return None
    



    def validate_config(self, config):
        required_keys = {"freq", "timer"}
        if not all(key in config for key in required_keys):
            return False
        return True




    def read_mon_data(self):

        if self.client is None:
            return None

        try:
            data_bme = bme.readReg()
            data_tla = tla.readAll()

            timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')

            data = {}
            data["type"] = "data"
            data["data_type"] = "mon_data"
            data["board"] = {

                "time": timestamp,
                "temp": data_bme[0],
                "pressure": data_bme[1],
                "hum": data_bme[2],
                "5V": (data_tla[0]/1000) * 3,
                "3V3": (data_tla[2]/1000) * 2,
                "I": data_tla[1]
            }
        
        except Exception as e:
            logger.critical(f"Unexpected problems when reading monitoring data: {e}")

        
        self.send_json(data)


    def auto_int(self, x):
        if isinstance(x, int): 
            return x
        return int(x, 0)

    def checkRegBoundary(self, addr):
      if (addr < 0 or addr > MAX_REG_ADDR):
         return False
      return True
    


    def read(self, addr):
        if (self.checkRegBoundary(self.auto_int(addr))):
            value = int.from_bytes(self.regs[self.auto_int(addr)*4:(self.auto_int(addr)*4)+4], byteorder='little')
            return (f'0x{value:08x}', value)
        else:
            return None



    def read_rates(self):  
        if self.client is None:
            return None
        
        try:
            reg_value = {}
            timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')

            reg_value["type"] = "data"
            reg_value["data_type"] = "rc_data"
            for reg in REGS:
                reg_value[reg] = {
                    "time": timestamp,
                    "value": self.read(reg)[1],
                    
                }
            data = reg_value

        except Exception as e:
            logger.critical(f"Unexpected problems when reading rates data: {e}")

        self.send_json(data)
        
        







if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", action="store", type=int, help="The port of the connection with the server (default:9000)", default=9000)
    parser.add_argument("--server_ip", action="store", type=str, help="The ip of the server (default:172.16.24.102)", default="172.16.24.102")
    parser.add_argument("--client_id", action="store", type=str, help="The id of the client (default:mon_249)", default="mon_249")

    args = parser.parse_args()

    client = Client(port=args.port, server_ip=args.server_ip, client_id=args.client_id)

    while True:
        client.start_connection()

        if not client.handshake():
            logger.error("Handshake failed. Exiting...")
            break

        result = client.send_data()
        if result != "Reconnect":
            logger.error("Client terminated due to an unexpected error")
            break
            

    



            
