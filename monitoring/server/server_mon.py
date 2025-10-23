import zmq
import logging
import datetime
import os
import csv
import json
import argparse
from pathlib import Path
from contextlib import contextmanager

#########################################
logger = logging.getLogger("Server")
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

#Error File Handler
server_error_handler = logging.FileHandler('server_error.log')
server_error_handler.setLevel(logging.WARNING)
server_error_handler.setFormatter(formatter)

logger.addHandler(server_error_handler)
#########################################



class Server:

    def __init__(self, port):
        self.port = port
        self.context = zmq.Context()
        self.server = None
        self.clients_connected = []
        self.opened_files = {}
        


    @staticmethod
    def generate_timestamp():
        return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')
    

    
    @staticmethod
    def get_file_name(data_type):
        timestamp = Server.generate_timestamp()
        file_map = {
            "rc_data": "rcmon",
            "hv_data": "hvmon",
            "mon_data": "mon"
        }

        file_prefix = file_map.get(data_type, "unknown")
        return f"{file_prefix}_{timestamp}.csv"
    


    @staticmethod
    def check_file_exists(fname):
        base, ext = os.path.splitext(fname)
        i = 1
        while os.path.exists(fname):
            fname = f"{base}_{i}{ext}"
            i += 1
        return fname
    


    def write_csv(self, client_id, data, data_type, batch_num):
        excluded_key = {"type", "data_type"}

        if data_type == "rc_data":
            with self.open_files(client_id, data_type, batch_num) as file_rc:
                writer = csv.writer(file_rc)
                for reg_address, reg_info in data.items():
                    if reg_address not in excluded_key:
                        writer.writerow([reg_address, reg_info["time"], reg_info["value"]])
            
        if data_type == "hv_data":
            with self.open_files(client_id, data_type, batch_num) as file_hv:
                writer = csv.writer(file_hv)
                for channel, channel_info in data.items():
                    if channel not in excluded_key:
                        writer.writerow([channel, channel_info["time"], channel_info["V"], channel_info["I"], channel_info["T"]])

        if data_type == "mon_data":
            with self.open_files(client_id, data_type, batch_num) as file_mon:
                writer = csv.writer(file_mon)
                for board, board_info in data.items():
                    if board not in excluded_key:
                        writer.writerow([board_info["time"], board_info["temp"], board_info["pressure"], board_info["hum"], board_info["5V"], board_info["3V3"], board_info["I"]])


    @contextmanager
    def open_files(self, client_id, data_type, batch_num):
        key = f"{client_id.decode()}_{data_type}"
        if key not in self.opened_files:
            if Path("/swgo").exists():
                base_path = Path("/swgo/multiPMT")
            else:
                base_path = Path.home() / "multiPMT"
                base_path.mkdir(parents=True, exist_ok=True)  # Crea Test se non esiste

            filepath = base_path / f"multi_{client_id.decode()}" / "monitoring" / self.check_file_exists(Server.get_file_name(data_type))
            filepath.parent.mkdir(parents=True, exist_ok=True)
            file = open(filepath, 'a', newline='')
            writer = csv.writer(file)

            if data_type == "rc_data":
                writer.writerow(['register', 'time', 'int_value'])

            if data_type == "hv_data":
                writer.writerow(['address', 'time', 'V', 'I', 'T'])

            if data_type == "mon_data":
                writer.writerow(['time', 'temp', 'pressure', 'hum', '5V', '3V3', 'I'])
            
            file.flush()
            self.opened_files[key] = file
        
        else:
            file = self.opened_files[key]

        try:
            yield file
        finally:
            file.flush()

    
    def save_data_csv(self, batch_num):
        try:
            message = self.server.recv_multipart()
            client_id = message[0]

            if client_id not in self.clients_connected:
                return
            
            if message[1] in {b"StopC", b"Reconnect"}:
                logger.info("Client requested stop/reconnect. Closing server...")
                self.server.close()
                return "Stop"

            try:
                data = json.loads(message[1].decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received from client {client_id}: {e}")
                return

            if data.get("type") == "data":
                data_type = data.get("data_type")
                self.write_csv(client_id, data, data_type, batch_num)
            
            else:
                logger.warning("Received message is not of type 'data', ignoring.")
        except Exception as e:
            logger.error(f"Error while saving data to CSV: {e}")

    
        


    def start_connection(self):

        try:
            self.server = self.context.socket(zmq.ROUTER)

            self.server.bind(f"tcp://*:{self.port}")
            logger.info(f"Server started on port {self.port}")

        except zmq.ZMQError as e: 
            logger.critical(f"Failed to bind socket on port {self.port}: {e}")
            self.server = None




    def handshake(self):

        if self.server is None:
            return False
    
        connected = False

        while not connected:

            try: 
                client_id, information = self.server.recv_multipart()

                if information==b"Ping":
                    self.server.send_multipart([client_id, b"Alive"])
                    

                    response = self.server.recv_multipart()
                    if response[0] == client_id and response[1] == b"Connection successful":
                        if client_id not in self.clients_connected:
                            self.clients_connected.append(client_id)

                        logger.info("Connection established successfully")
                        connected = True
                        return True

                    else:
                        logger.error("Handshake failed: mismatched response")
                        
                else:
                    logger.error("Unexpected message or unknown client ID")
                    


            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error during handshake: {e}")
                

            except Exception as e:
                logger.critical(f"Unexpected error during handshake: {e}")




    def clean_up(self):
        for file in self.opened_files.values():
            file.close()

        self.opened_files.clear()
        self.clients_connected.clear()

        if self.server:
            self.server.close()

    

    def handle_clients(self, freq, timer, batch_num):

        if not self.clients_connected:
            logger.warning("No connected clients to handle.")
            return
        
        for client_id in self.clients_connected:
            logger.info(f"Handling connection with client {client_id.decode()}")
            params = json.dumps({"type": "config", "freq": freq, "timer": timer})
            self.server.send_multipart([client_id, params.encode("utf-8")])

        poller = zmq.Poller()
        poller.register(self.server, zmq.POLLIN)


        try:
            while True:
                events = dict(poller.poll())
                if self.server in events:
                    check = self.save_data_csv(batch_num)
                    if check == "Stop":
                        self.clean_up()
                        return "Reconnect"
        
        except KeyboardInterrupt:
            logger.info("Interrupt received. Stopping server...")
            self.clean_up()
        except zmq.ZMQError as e:
            logger.error(f"Connection error with client: {e}")
            self.clean_up()
        except Exception as e:
            logger.critical(f"Unexpected error: {e}")
            self.clean_up()

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("batch", type=int, help="The number of the batch")
    parser.add_argument("--freq", action="store", type=int, help="Time between two measurements (default:10 s)", default=10)
    parser.add_argument("--timer", action="store", type=int, help="Timer of measurements (default:None)", default=None)
    parser.add_argument("--port", action="store", type=int, help="The port of the connection with the server (default:9000)", default=9000)

    args = parser.parse_args()

    server = Server(port=args.port)
    while True:
        server.start_connection()
        if not server.handshake():
            logger.error("Handshake failed. Exiting...")
            break
        
        result = server.handle_clients(args.freq, args.timer, args.batch)
        if result != "Reconnect":
            logger.error("Client terminated due to an unexpected error")
            break

        



    



            
