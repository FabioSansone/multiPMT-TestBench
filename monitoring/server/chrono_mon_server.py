import zmq
import logging
import datetime
import os
import csv
import json
import time
import argparse
import sys
from pathlib import Path
from contextlib import contextmanager

#########################################
logger = logging.getLogger("Chrono_server")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

#Error File Handler
server_error_handler = logging.FileHandler('chrono_server_error.log')
server_error_handler.setLevel(logging.DEBUG)
server_error_handler.setFormatter(formatter)

logger.addHandler(server_error_handler)
#########################################


#########################################

class Server:

    def __init__(self, port):
        self.port = port                    #Connection port with the client on Linux Systems
        self.context = zmq.Context()
        self.server = None
        self.clients_connected = []
        self.opened_files = {}
        

    @staticmethod
    def check_window(start_time, stop_time, now=None):
        
        if now is None:
            now = datetime.datetime.now()

        t = now.time()

        if start_time <= stop_time:
            return start_time <= t < stop_time
        else:
           return t >= start_time or t < stop_time
    


    @staticmethod
    def next_occurrence_time(target_time, base_dt=None):
        """
        Questa funzione serve per calcolare ogni volta la prossima occorrenza
        di start time e di stop time tenendo conto dell'attraversamento del giorno
        """
        if base_dt is None:
            base_dt = datetime.datetime.now()
        candidate = datetime.datetime.combine(base_dt.date(), target_time)
        if candidate <= base_dt:
            candidate += datetime.timedelta(days=1)
        return candidate
    


    @staticmethod
    def wait_until_datetime(target_dt):
        while 1:
            now = datetime.datetime.now()
            if now >= target_dt:
                return
            time.sleep(10)

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



    @contextmanager
    def open_files(self, client_id, data_type):
        key = f"{client_id.decode()}_{data_type}"
        if key not in self.opened_files:
            if Path("/swgo").exists():
                base_path = Path("/swgo/multiPMT")
            else:
                base_path = Path.home() / "multiPMT"
                base_path.mkdir(parents=True, exist_ok=True)  # Crea Test se non esiste
            
            fname = Server.get_file_name(data_type)
            filepath = base_path / f"multi_{client_id.decode()}" / "monitoring" / fname
            filepath = Path(self.check_file_exists(filepath))
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
        
    

    def save_data_csv(self):
        try:
            message = self.server.recv_multipart()
            client_id = message[0]

            if client_id not in self.clients_connected:
                return
            
            if message[1] == b"StopC":
                logger.info("Client requested stop. Closing server...")
                self.server.close()
                return "Stop"
            
            elif message[1] == b"Reconnect":
                logger.info("Client requested reconnect. Cleaning up but keeping server alive.")
                self.clean_up()
                return "Reconnect"

            try:
                data = json.loads(message[1].decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received from client {client_id}: {e}")
                return

            if data.get("type") == "data":
                data_type = data.get("data_type")
                self.write_csv(client_id, data, data_type)
            
            else:
                logger.warning("Received message is not of type 'data', ignoring.")
        except Exception as e:
            logger.error(f"Error while saving data to CSV: {e}")
    


    def write_csv(self, client_id, data, data_type):
        excluded_key = {"type", "data_type"}

        if data_type == "rc_data":
            with self.open_files(client_id, data_type) as file_rc:
                writer = csv.writer(file_rc)
                for reg_address, reg_info in data.items():
                    if reg_address not in excluded_key:
                        writer.writerow([reg_address, reg_info["time"], reg_info["value"]])
            
        if data_type == "hv_data":
            with self.open_files(client_id, data_type) as file_hv:
                writer = csv.writer(file_hv)
                for channel, channel_info in data.items():
                    if channel not in excluded_key:
                        writer.writerow([channel, channel_info["time"], channel_info["V"], channel_info["I"], channel_info["T"]])

        if data_type == "mon_data":
            with self.open_files(client_id, data_type) as file_mon:
                writer = csv.writer(file_mon)
                for board, board_info in data.items():
                    if board not in excluded_key:
                        writer.writerow([board_info["time"], board_info["temp"], board_info["pressure"], board_info["hum"], board_info["5V"], board_info["3V3"], board_info["I"]])



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
    



    def handle_clients(self, freq, timer, timestamp_ref, stop_dt):

        if not self.clients_connected:
            logger.warning("No connected clients to handle.")
            return
        
        for client_id in self.clients_connected:
            logger.info(f"Handling connection with client {client_id.decode()}")
            params = json.dumps({"type": "config", "freq": freq, "timer": timer, "timestamp_ref": timestamp_ref})
            self.server.send_multipart([client_id, params.encode("utf-8")])

            self.server.send_multipart([client_id, b"Start"])

        logger.info("Sent 'Start' signal to all clients")

        poller = zmq.Poller()
        poller.register(self.server, zmq.POLLIN)


        try:
            while True:
                events = dict(poller.poll())
                if self.server in events:
                    check = self.save_data_csv()
                    if check == "Stop":
                        self.clean_up()
                        return "Stop"
                    elif check == "Reconnect":
                        self.clean_up()
                        return "Reconnect"
                
                if stop_dt is not None and datetime.datetime.now() >= stop_dt:
                    logger.info("Scheduled stop time reached inside handle_clients. Cleaning up.")
                    self.clean_up()
                    return "ScheduledStop"

        except KeyboardInterrupt:
            logger.info("Interrupt received. Stopping server...")
            self.clean_up()
        except zmq.ZMQError as e:
            logger.error(f"Connection error with client: {e}")
            self.clean_up()
        except Exception as e:
            logger.critical(f"Unexpected error: {e}")
            self.clean_up()
    


    def clean_up(self):
        for client_id in self.clients_connected:
            try:
                self.server.send_multipart([client_id, b"Stop"])
            except zmq.ZMQError:
                logger.warning(f"Could not send Stop to {client_id.decode()} (socket likely closed)")

        logger.info("Sent 'Stop' signal to all clients")

        for file in self.opened_files.values():
            file.close()

        self.opened_files.clear()




if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--freq", action="store", type=int, help="Time between two measurements (default:10 s)", default=10)
    parser.add_argument("--timer", action="store", type=int, help="Timer of measurements (default:None)", default=None)
    parser.add_argument("--port", action="store", type=int, help="The port of the connection with the server (default:9000)", default=9001)

    parser.add_argument("--start", type=str, help = "Start time in format HH:MM (default = 20:00)", required=True, default="20:00")
    parser.add_argument("--stop", type=str, help = "Stop time in format HH:MM (default = 5:00)", required=True, default="5:00")

    args = parser.parse_args()

    start_time = datetime.datetime.strptime(args.start, "%H:%M").time()
    stop_time = datetime.datetime.strptime(args.stop, "%H:%M").time()

    server = Server(port=args.port)
    server.start_connection()
    if not server.handshake():
        logger.error("Handshake failed. Exiting...")
        sys.exit(-1)

    while True:
        
        now = datetime.datetime.now()

        if server.check_window(start_time, stop_time, now):
            start_dt = now
            logger.info("Current time is inside the configured window: starting immediately.")
        else:
            start_dt = Server.next_occurrence_time(start_time, now) #Calcolo con la funzione quando devo incominciare tenendo conto del target che sarebbe lo start e del tempo di adesso
            logger.info(f"Waiting until start time: {start_dt.isoformat()}")
            server.wait_until_datetime(start_dt)
            logger.info("Start time reached, beginning data collection")

        stop_dt = server.next_occurrence_time(stop_time, start_dt) #Tenendo conto dello start calcolo quando devo terminare
        logger.info(f"Scheduled stop at: {stop_dt.isoformat()}")

        timestamp_ref = server.generate_timestamp()

        result = server.handle_clients(args.freq, args.timer, timestamp_ref, stop_dt=stop_dt)

        if result == "Reconnect":
            continue
        elif result == "ScheduledStop":
            logger.info("Scheduled stop reached. Exiting main loop.")
            continue
        elif result == "Stop":
            logger.info("Client requested StopC. Exiting server permanently.")
            break
        else:
            logger.error("Client terminated due to an unexpected error")
            break
