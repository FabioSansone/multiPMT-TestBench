import zmq
import logging
import datetime
import os
import struct
import csv
import time
from pathlib import Path

#########################################
logger = logging.getLogger("DataProcessing")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Error File Handler
processor_error_handler = logging.FileHandler('processor_error.log')
processor_error_handler.setLevel(logging.DEBUG)
processor_error_handler.setFormatter(formatter)
logger.addHandler(processor_error_handler)
#########################################

# In milliseconds
HEARTBEAT_IVL = 60000
HEARTBEAT_TIMEOUT = 120000
HEARTBEAT_TTL = 300000

#Dictionary to set the folder based on the acquisition type => "acquisition_type" : "folder_name"

folder_acq = {


    "polarizer" : "polarizer_calibration/",
    "pedestal" : "pedestal_characterisation/",
    "spe" : "single_photoelectron/",
    "gain" : "gain_curve/",
    "wheels_char" : "wheels_characterisation/",
    "fiber_char" : "fiber_characterisation/"
}

class DataProcess:

    def __init__(self, port=5555):
        self.port = port
        self.context = zmq.Context()
        self.server = None
        self.opened_files = []
        logger.debug("DataProcess initialized with port %s", self.port)

    @staticmethod
    def generate_timestamp():
        return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')
    
    @staticmethod
    def generate_timestamp_folder():
        return datetime.datetime.now().strftime('%Y_%m_%d')
    
    @staticmethod
    def get_file_name(suffix):
        timestamp = DataProcess.generate_timestamp()
        file_prefix = "daq"
        return f"{file_prefix}_{timestamp}_{suffix}.csv"
    
    @staticmethod
    def check_file_exists(fname):
        base, ext = os.path.splitext(fname)
        i = 1
        while os.path.exists(fname):
            fname = f"{base}_{i}{ext}"
            i += 1
        return fname

    def start_connection(self):
        try:
            self.server = self.context.socket(zmq.ROUTER)
            self.server.setsockopt(zmq.HEARTBEAT_IVL, HEARTBEAT_IVL)
            self.server.setsockopt(zmq.HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT)
            self.server.setsockopt(zmq.HEARTBEAT_TTL, HEARTBEAT_TTL)
            self.server.setsockopt(zmq.RCVTIMEO, 60000)
            self.server.bind(f"tcp://*:{self.port}")
            logger.info(f"Server started on port {self.port}")
        except zmq.ZMQError as e: 
            logger.critical(f"Failed to bind socket on port {self.port}: {e}")
            self.server = None

    def clean_up(self):
        logger.debug("Cleaning up opened files and sockets")
        self.opened_files.clear()
        if self.server:
            self.server.close()
            self.context.term()
            logger.debug("Server and context cleared")

        logger.info("Everything has been cleared")

    def clean_up_fifo(self):
        logger.debug("Cleaning up opened  sockets")
        if self.server:
            self.server.close()
            logger.debug("Server cleared")

        logger.info("Everything has been cleared")

    @staticmethod
    def get_file_path(acq_type, number):
        base_folder = Path.home() / "multiPMT" / "calibration" / f"batch_{number}" / folder_acq.get(acq_type, "unknown") / DataProcess.generate_timestamp_folder()
        i = 1
        folder = base_folder / f"acq_{i}"
        while folder.exists():
            i += 1
            folder = base_folder / f"acq_{i}"
        return folder
    
    def string_no_space(self, string):
        return string.replace(" ", "")



    def run(self, duration=None, suffix="", flag_acq = "", run_id = None, number = None): 
        self.start_connection()
        if not self.server:
            logger.error("Server is not initialized. Exiting run method.")
            return
        
        poller = zmq.Poller()
        poller.register(self.server, zmq.POLLIN)  # Controlla se ci sono dati disponibili
        
        base_folder = Path("/swgo") / "multiPMT" / "calibration" / f"batch_{number}" / folder_acq.get(flag_acq, "unknown") / DataProcess.generate_timestamp_folder()
        
        if run_id is not None:
            run_folder = base_folder / f"run_{run_id}"
        else:
            i = 1
            run_folder = base_folder / f"acq_{i}"
            while run_folder.exists():
                i += 1
                run_folder = base_folder / f"acq_{i}"

        run_folder.mkdir(parents=True, exist_ok=True)

        filename = self.check_file_exists(DataProcess.get_file_name(suffix))
        filepath = run_folder / filename
        filepath = Path(filepath).expanduser()
        
        with open(filepath, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Channel", "Unix_time_16_bit", "Coarse_time", "TDC_time", "ToT_time", "TDC_trigger_end", "Energy", "CRC"])
            
            start_time = time.time()
            logger.info("Starting the communication with the DMA")
            while duration is None or time.time() - start_time < duration:
                socks = dict(poller.poll(timeout=5000))  
                
                if self.server in socks and socks[self.server] == zmq.POLLIN:
                    try:
                        message = self.server.recv_multipart()
                    except zmq.ZMQError as e: 
                        logger.error("Failed to receive messages: %s", e)
                        continue
                    
                    for part in message:
                        if len(part) != 1:
                            l = int(len(part) / 2)
                            try:
                                v = struct.unpack_from(f"{l}H", part)
                            except Exception as ex:
                                logger.error("Error in struct.unpack_from: %s", ex)
                                continue 
                            
                            a = ""
                            i = 0
                            for b in v:
                                value = f'{b:04x} ' 
                                i += 1
                                a += value
                                if i % 8 == 0: 
                                    try:
                                        self.process_data(a.strip(), writer)
                                        file.flush()
                                        a = ""
                                        i = 0
                                    except Exception as e:
                                        logger.error(f"Some problems occured when putting the data in the queue: {e}")
                else:
                    logger.debug("No message received in 5 seconds. Continuing...")

            file.flush()
            logger.info("Closing and flushing file. Starting clean up")        
            self.clean_up()
            logger.info("DataProcess.run terminated")

    
    def process_data(self, event, writer):

        if not event:
            logger.info("Empty event received. Skipping...")
            return

        event = self.string_no_space(event)
        event_cut = event[4:-4]
        if not event_cut: 
            logger.info("The event received has no valuable data. Skipping...")
            return 
        
        number_bits = len(event_cut) * 4
        event_integer = int(event_cut, 16)
        binary_string = bin(event_integer)[2:]
        event_bit = binary_string.zfill(number_bits)
        
        try:
            canale = int(event_bit[3:8],2)
            tempo_16_bit = int(event_bit[8:24],2)
            coarse_time = int(event_bit[24:32] + event_bit[33:40] + event_bit[40:53], 2)
            tot = int(event_bit[53:59], 2)
            tdc_trigger_end = int(event_bit[59:64], 2)
            tdc_time = int(event_bit[69:74], 2)
            energia = int(event_bit[74:88], 2)
            crc = int(event_bit[88:96], 2)
            
            writer.writerow([
                str(canale),
                str(tempo_16_bit),
                str(coarse_time),
                str(tdc_time),
                str(tot),
                str(tdc_trigger_end),
                str(energia),
                str(crc)
            ])
        except Exception as e:
            logger.error(f"Error parsing event: {e}")




    
    def flush_fifo(self, duration=60): 
        self.start_connection()
        if not self.server:
            logger.error("Server is not initialized. Exiting run method.")
            return
         
            
        start_time = time.time()
        logger.info("Starting the communication with the DMA to empty the FIFO")
        while duration is None or time.time() - start_time < duration:
            try:
                message = self.server.recv_multipart()
            except zmq.Again:
                logger.debug("No message received in 5 seconds. Retrying...")
                continue
            except zmq.ZMQError as e: 
                logger.error("Failed to receive messages: %s", e)
                continue
        
        logger.info("Starting clean up after empting fifo")        
        self.clean_up_fifo()
        logger.info("Empting fifo terminated")


    

    def signal_integrity(self, duration=60): 

        self.start_connection()
        if not self.server:
            logger.error("Server is not initialized. Exiting run method.")
            return
        
        poller = zmq.Poller()
        poller.register(self.server, zmq.POLLIN)  # Controlla se ci sono dati disponibili
        
        energy_info = {ch : [] for ch in range(7)}
       
      
        start_time = time.time()
        logger.info("Starting the communication with the DMA")
        while duration is None or time.time() - start_time < duration:
            socks = dict(poller.poll(timeout=5000))  
                
            if self.server in socks and socks[self.server] == zmq.POLLIN:
                try:
                    message = self.server.recv_multipart()
                except zmq.ZMQError as e: 
                    logger.error("Failed to receive messages: %s", e)
                    continue
                    
                for part in message:
                    if len(part) != 1:
                        l = int(len(part) / 2)
                        try:
                            v = struct.unpack_from(f"{l}H", part)
                        except Exception as ex:
                            logger.error("Error in struct.unpack_from: %s", ex)
                            continue 
                            
                        a = ""
                        i = 0
                        for b in v:
                            value = f'{b:04x} ' 
                            i += 1
                            a += value
                            if i % 8 == 0: 
                                try:

                                    self.process_signal_integrity(a.strip(), energy_info)
                                    a = ""
                                    i = 0
                                except Exception as e:
                                    logger.error(f"Some problems occured when putting the data in the queue: {e}")
            else:
                logger.debug("No message received in 5 seconds. Continuing...")

            
        

        energy_means = {ch: (sum(energy) / len(energy) if energy else 0) for ch, energy in energy_info.items()}
        valid_channels = sum(1 for mean in energy_means.values() if mean > 1000)

        if valid_channels >= 4:
            logger.info(f"Signal integrity check PASSED: {valid_channels} channels have mean energy > 1000.")
            logger.info("Starting clean up")        
            self.clean_up()
            logger.info("DataProcess.signal_integrity terminated")
            return True
        else:
            logger.warning(f"Signal integrity check FAILED: only {valid_channels} channels have mean energy > 1000.")
            logger.info("Starting clean up")        
            self.clean_up()
            logger.info("DataProcess.signal_integrity terminated")
            return False

        
        

    
    def process_signal_integrity(self, event, data_save):

        if not event:
            logger.info("Empty event received. Skipping...")
            return

        event = self.string_no_space(event)
        event_cut = event[4:-4]
        if not event_cut: 
            logger.info("The event received has no valuable data. Skipping...")
            return 
        
        number_bits = len(event_cut) * 4
        event_integer = int(event_cut, 16)
        binary_string = bin(event_integer)[2:]
        event_bit = binary_string.zfill(number_bits)
        
        try:
            canale = int(event_bit[3:8],2)
            energia = int(event_bit[74:88], 2)
            
            data_save[canale].append(energia)

        except Exception as e:
            logger.error(f"Error parsing event: {e}")





if __name__ == "__main__":
    test = DataProcess()
    test.run(60, "test", "test", 1)
