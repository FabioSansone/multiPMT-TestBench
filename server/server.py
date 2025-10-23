#!/usr/bin/env python3
#coding=utf-8
import cmd2
import zmq
import argparse
import logging
import json
import time
import threading
import HardwareResources
from InstrumentManager import InstrumentsManager
from data_processing import DataProcess
import MonitoringProcessing
import socket
from pathlib import Path
from functools import wraps, partial



#Dictionary with MainBoard infos
main_info = {'cile1' : 'Main_v1_SN02_cile1',
             'cile2' : 'Main_v1_SN05_cile2',
             'rio' : 'Main_v1_SN03_rio',
             'milano': 'Main_v1_SN04_milano'}

#Generic Constants
MAX_RETRIES = 3
DISCOVERY_PORT = 8001
CHANNELS = 7

#ZMQ Constants
POLLER_TIMEOUT_CONNECTION = 20000 #in ms


##################################
# LOGGER
##################################
logger = logging.getLogger("Server")
logger.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Error File Handler
server_error_handler = logging.FileHandler('server_error.log')
server_error_handler.setLevel(logging.ERROR)
server_error_handler.setFormatter(formatter)
logger.addHandler(server_error_handler)
##################################


class Server(cmd2.Cmd):
    "A terminal application to switch and interact with different multiPMT"

    intro = "Welcome to the control interface for the multiPMTs. Type ? or help to list commands."
    prompt = "|Server> "

    def __init__(self, context) -> None:
        super().__init__()
        self.context = context
        self.main_id = None
        self.flag_status_file = 0
        self.flag_acq_multi = 1
        self.flag_test = None
        self.server = None
        self.clients_connected = []  
        self.instrument_manager = InstrumentsManager(self.poutput)
        self.batch = None   
        self.path_mbfile = None
        

        self.discovery_stop_event = threading.Event()
        self.discovery_thread = threading.Thread(
            target=self._udp_discovery_listener,
            args=(self.discovery_stop_event,),
            daemon=True
        )
        self.discovery_thread.start()


    
    ##########################################
    # SERVER-CLIENT COMMUNICATION
    ##########################################

    def _udp_discovery_listener(self, stop_event):
        """
        Thread to handle UDP discovery calls.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.bind(('', DISCOVERY_PORT))

            self.poutput(f"[UDP] Discovery listener started on port {DISCOVERY_PORT}")

            while not stop_event.is_set():
                try:
                    s.settimeout(1.0)  # breve timeout per poter controllare stop_event
                    data, addr = s.recvfrom(1024)
                    if data == b"DISCOVER_SERVER":
                        self.poutput(f"[UDP] Ricevuto DISCOVER_SERVER da {addr[0]}")
                        s.sendto(b"I am server", addr)
                except socket.timeout:
                    continue
                except Exception as e:
                    self.poutput(f"[UDP] Errore durante la gestione del discovery: {e}")
                    

    def _start_connection(self, port = DISCOVERY_PORT):
        """Starts the connection with """
        try:
            self.server = self.context.socket(zmq.ROUTER)
            port = port #multiPMT_port[ip]
            self.server.bind(f"tcp://*:{port}")
            self.poutput(f"Server started on port {port}")
        except zmq.ZMQError as e:
            logger.error(f"Failed to bind socket on port {port}: {e}")
            self.poutput(f"Error: {e}")
            self.server = None

    def _handshake_attempt(self, flag_test:str):

        """
        Performs a single handshake attempt with a client.
        Uses zmq.Poller to handle the timeout.
        Returns True if the handshake is successful, False otherwise.
        """

        if self.server is None:
            self.poutput("Server not started. Cannot perform handshake.")
            return False

        poller = zmq.Poller()
        poller.register(self.server, zmq.POLLIN)

        try: 
            socks = dict(poller.poll(POLLER_TIMEOUT_CONNECTION))
            if self.server not in socks:
                self.poutput("Timeout waiting for handshake message.")
                return False 

            client_id, message = self.server.recv_multipart()

            if message != b"Ping":
                self.poutput("Unexpected message during handshake.")
                return False
            

            self.server.send_multipart([client_id, b"Alive"])
            socks = dict(poller.poll(POLLER_TIMEOUT_CONNECTION))
            if self.server not in socks:
                self.poutput("Timeout waiting for connection confirmation.")
                return False
            

            response = self.server.recv_multipart()
            if response[0] != client_id or response[1] != b"Connection successful":
                self.poutput("Handshake failed: Incorrect connection response.")
                return False

            if client_id not in self.clients_connected:
                self.clients_connected.append(client_id)

            self.poutput("Connection established successfully")

            self.server.send_multipart([client_id, b"EV"])
            socks = dict(poller.poll(POLLER_TIMEOUT_CONNECTION)) 
            if self.server not in socks:
                self.poutput("Timeout in attesa della risposta EV.")
                return False

            ev_response = self.server.recv_multipart()  
            if ev_response[0] != client_id or ev_response[1] != b"EV Success":
                self.poutput("Handshake EV fallito.")
                return False
            
                   
            self.poutput("Evproducer and the Run Control have been set up")

            self.server.send_multipart([client_id, json.dumps(flag_test).encode("utf-8")])
            socks = dict(poller.poll(POLLER_TIMEOUT_CONNECTION * 30)) #Wait 10 minutes to let the client set evproducer and the high voltage
            if self.server not in socks:
                self.poutput("Timeout in attesa della HV o del setting della configurazione di test.")
                return False
            
            final_response = self.server.recv_multipart()
            if final_response[0] != client_id or (final_response[1] != b"HV Success" and final_response[1] != b"Test Success"):
                self.poutput("Handshake HV/Test fallito.")
                return False
            
            self.poutput("Everything has been set correctly. The system is ready and properly connected.")
            self.poutput(f"This is the list of the connected clients: {self.clients_connected}")
            return True
                    
        except zmq.ZMQError as e:
            self.poutput(f"ZMQ Error during handshake: {e}")
            return False
        except Exception as e:
            self.poutput(f"Unexpected error during handshake: {e}")
            return False
    
    def _handshake(self, num_clients, flag_test:str):
        """
        Loops through handshake attempts until num_clients clients connect.
        Up to MAX_RETRIES attempts are made for each handshake.
        """ 
        self.poutput(f"Attesa di {num_clients} client...")

        while len(self.clients_connected) < num_clients:
            retries = 0
            success = False

            while retries < MAX_RETRIES and not success:
                self.poutput(f"Tentativo handshake con un client (tentativo {retries+1}/{MAX_RETRIES})")
                success = self._handshake_attempt(flag_test)
                if not success:
                    retries += 1
                    self.poutput("Tentativo di handshake fallito, riprovo...")

            if not success:
                self.poutput("Handshake non riuscito dopo il numero massimo di tentativi. Procedura interrotta.")
                break 
        
        if len(self.clients_connected) == num_clients:
            self.poutput("Tutti i client si sono connessi con successo!")
            return success
        else:
            self.poutput(f"Numero di client connessi: {len(self.clients_connected)} (attesi {num_clients})")
            return False


    def _clean_up(self):
        """
        Clean up funtion to realise all the resources
        """
        try:
            self.clients_connected.clear()
            if self.server:
                self.server.close()
            self.discovery_stop_event.set()
            if self.discovery_thread.is_alive():
                self.discovery_thread.join(timeout=5.0)
            self.context.term()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    ##########################################
    # INSTRUMENTS
    ##########################################

    def _init_wheels(self, near_wheel_pos, far_wheel_pos):
        self.instrument_manager.init_wheels(near_wheel_pos, far_wheel_pos)

    def _init_polarizer(self, pol_position):
        self.instrument_manager.init_polarizer(pol_position)

    ###############################
    # RC
    ###############################

    def _rc_write(self, addr, value):
        HardwareResources.RCWrite(self.server, self.clients_connected, addr, value, self.poutput)

    ###############################
    # HV
    ###############################

    def _set_init_conf(self, channels, port="/dev/ttyPS1", voltage_set=None, threshold_set=100, limit_trip_time=2, limit_voltage=100, limit_current=5, limit_temperature=50, rate_up=25, rate_down=25):
        HardwareResources.HVSetInitConf(socket=self.server, clients=self.clients_connected, port=port, channels=channels, 
                                        voltage_set=voltage_set, threshold_set=threshold_set, limit_trip_time=limit_trip_time,
                                        limit_voltage=limit_voltage, limit_current=limit_current, limit_temperature=limit_temperature,
                                        rate_up=rate_up, rate_down=rate_down, output_func=self.poutput)
        

    def _set_voltage(self, channels, voltage, port="/dev/ttyPS1"):
        HardwareResources.HVSetVoltage(socket=self.server, clients=self.clients_connected, port=port, channels=channels, voltage=voltage, output_func=self.poutput)
    
    def _set_threshold(self, channels, threshold, port="/dev/ttyPS1"):
        HardwareResources.HVSetThreshold(socket=self.server, clients=self.clients_connected, port=port, channels=channels, threshold=threshold, output_func=self.poutput)

    def _pwr_on(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVPowerOn(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)
    
    def _pwr_off(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVPowerOff(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)

    def _hv_calib(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVCalibration(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)

    def _hv_serial(self, channels, port="/dev/ttyPS1"):
        serial_info = HardwareResources.HVGetSerialFEB(socket=self.server, clients=self.clients_connected, port=port, channels=channels, batch=self.batch, output_func=self.poutput)
        return serial_info
    
    def _hv_set_serial(self, channels, serials, port="/dev/ttyPS1"):
        serial_info = HardwareResources.HVSetSerialPMT(socket=self.server, clients=self.clients_connected, port=port, channels=channels, serials=serials, batch=self.batch, output_func=self.poutput)
        return serial_info
    
    def _hv_prog_feb(self, channels, firmware="HKL031V4B.hex", baud=115200, port="/dev/ttyPS1"):
        HardwareResources.HVProgFEB(socket=self.server, clients=self.clients_connected, port=port, channels=channels, baud=baud, firmware=firmware, output_func=self.poutput)
    
    
    ###############################
    # DAQ
    ###############################

    def _acquire_charge(self, suffix, flag_acq, run_id = None, timer=60):   
        charge = DataProcess()
        HardwareResources.DMACommunication(socket=self.server, clients=self.clients_connected, charge=charge, suffix=suffix, flag_acquisition=flag_acq, 
                                            run_id=run_id, timer=timer, batch=self.batch, output_func=self.poutput)
    
    def _check_signal(self):
        charge = DataProcess()
        check = HardwareResources.SignalIntegrity(socket=self.server, clients=self.clients_connected, charge=charge, output_func=self.poutput)
        return check

    def _acquire_rate(self, registers, flag_acq, suffix, run_id):
        HardwareResources.RCMonitoring(socket=self.server, clients=self.clients_connected, registers=registers, batch=self.batch, flag_acq=flag_acq, suffix=suffix, run_id = run_id, output_func=self.poutput)

    def _monitoring_all(self, rc_flag, hv_flag, mon_flag, flag_acq, suffix, run_id):
        HardwareResources.Monitoring(socket=self.server, clients=self.clients_connected, rc_flag=rc_flag, hv_flag=hv_flag, mon_flag=mon_flag, batch=self.batch, 
                                     flag_acq=flag_acq, suffix=suffix, run_id=run_id, output_func=self.poutput)

    ###############################
    # START UP  
    ###############################

    def _prog_feb(self, channels, port):
        self.poutput("The FEBs will be programmed\n")
        self._hv_prog_feb(channels=channels, firmware="HKL031V4B.hex", baud=115200, port=port)
        self.poutput("The FEBs have been programmed correctly")

    def _info_file_check(self, ):

        
        base_path = Path('/swgo') if Path('/swgo').exists() else Path.home() / 'multiPMT'
        base_folder = base_path / 'configuration_files'

        base_folder.mkdir(parents=True, exist_ok=True)

        if self.main_id.lower() not in main_info:
            self.poutput(f"Main ID '{self.main_id}' not recognised")
            raise KeyError
            
        
        board_folder = base_folder / main_info[self.main_id.lower()]
        board_folder.mkdir(parents=True, exist_ok=True)
        
        mb_file = board_folder / f'{main_info[self.main_id.lower()]}.json'
        
        self.path_mbfile = mb_file
        
        info_per_channel = {'PMT_Serial': None,
                            'Device_ID' : None,
                            'Voltage': None,
                            'Very_Low_Threshold': None, #1/3 pe
                            'Low_Threshold': None, #1 pe
                            'Medium_Threshold': None, #2 pe
                            'High_Threshold': None} #4 pe
        
        required_keys = ['Device_ID', 'Voltage', 'Very_Low_Threshold', 'Medium_Threshold']
        
        conf_structure = {i : info_per_channel.copy() for i in range(CHANNELS)}
        
        
        if mb_file.exists():
            try:
                with open(mb_file, "r") as f:
                    from_file_info = json.load(f)
                    
                valid = True
                for channel in range(CHANNELS):
                    for key in required_keys:
                        value = from_file_info.get(str(channel), {}).get(key, None)
                        if value is None or value == '':
                            valid = False
                            break
                    if not valid:
                        break

                if valid:
                    self.flag_status_file = 1  # tutto ok, non rigenerare
                    self.poutput(f"Configuration exists and is valid {self.main_id}")
                    return 

            except Exception as e:
                self.poutput(f"Errore nel parsing del file: {e}")
                            
                    
        
        try:
            pmt_info = self._hv_serial('all')
            for channel in pmt_info.keys():
                #conf_structure[channel]['PMT_Serial'] = pmt_info[channel][1] #pmt_info che viene dal client è un dizionario con canale -> lista di informazioni
                conf_structure[channel]['Device_ID'] = pmt_info[channel][0]
        
            with open(mb_file, "w", newline="") as f:
                json.dump(conf_structure, f, indent=4)
        except Exception as e:
            self.poutput("A problem occured during info file creation. Exiting...")
            return
         
        return 
    
    
    def update_channel_info(self, updates: dict):
        """
        Aggiorna uno o più campi del file JSON dei canali.

        :param updates: dict con chiavi = nomi dei campi da aggiornare
                        valori = lista di valori per ogni canale.
                        Esempio:
                        {
                            "PMT_Serial": ["AAA", "BBB", ...],
                            "Voltage": [1100, 1150, ...]
                        }
        """
        
        with open(self.path_mbfile, 'r', newline='') as f:
            file_info = json.load(f)

        for field, values in updates.items():
            if len(values) != CHANNELS:
                raise ValueError(f"The field '{field}' must have {CHANNELS} values")
            
            already_written = [
            i for i in range(CHANNELS)
            if file_info.get(str(i), {}).get(field) not in [None, ""]
            ]
            
            if already_written:
                ans = input(f"Channels {already_written} already have a value for '{field}'. Overwrite? [y/N] ").strip().lower()
                if ans.lower() not in ['y', 'yes']:
                    self.poutput('Process was halted by user command')
                    return
            for i in range(CHANNELS):
                ch_key = str(i)
                if ch_key not in file_info:
                    file_info[ch_key] = {}  
                if field not in file_info[ch_key]:
                    file_info[ch_key][field] = None  
                file_info[ch_key][field] = values[i]
                
           
        with open(self.path_mbfile, 'w', newline='') as f:
            json.dump(file_info, f, indent=4)
        
        
    def setup_voltage(self, args, subcmd=None):
        return {'Voltage': args.values}

    def setup_very_low_threshold(self, args, subcmd=None):
        return {'Very_Low_Threshold': args.values}

    def setup_low_threshold(self, args, subcmd=None):
        return {'Low_Threshold': args.values}

    def setup_medium_threshold(self, args, subcmd=None):
        return {'Medium_Threshold': args.values}

    def setup_high_threshold(self, args, subcmd=None):
        return {'High_Threshold': args.values}

    def setup_pmt_serial(self, args, subcmd=None):
        return {'PMT_Serial': args.values}
    
    
    def check_pmt_serials(self, serials):
        pmts_info = self._hv_serial('all')
        yes_pmt_serial = [
        i for i in range(CHANNELS)
        if pmts_info[i][1] not in [None, ""]
        ]
        
        if yes_pmt_serial:
            ans = input(f"Channels {yes_pmt_serial} already have a saved serial'. Overwrite? [y/N] ").strip().lower()
            if ans.lower() not in ['y', 'yes']:
                self.poutput('Process was halted by user command')
                return
            
        if self._hv_set_serial(CHANNELS, serials=serials):
            self.poutput("Serial Numbers set successfully")
    
    
    
    def command_guard(category):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                test = getattr(self, 'flag_test', 0)
                multi = getattr(self, 'flag_acq_multi', 0)
                status = getattr(self, 'flag_status_file', 0)

                # Case 1: System not secured -> Only setup and safety
                if test == 1:
                    if category not in ["setup", "safety_only", "safety"]:
                        self.poutput("System not in a safe condition (flag_test = 1). Only setup and safety commands are allowed.")
                        return
                    else:
                        return func(self, *args, **kwargs)

                # Case 2: System secured

                # safety_only is always valid
                if category == "safety_only":
                    return func(self, *args, **kwargs)

                # multiPMT acquisition mode
                if multi == 1:
                    if not status and category != "setup":
                        self.poutput("Configuration incomplete. Please execute the setup first.")
                        return
                    elif status and category != "global_acq" and category != "setup":
                        self.poutput("This command is not available in full acquisition mode.")
                        return

                # PMTs characterisation mode
                elif multi == 0:
                    if category not in ["characterization", "setup"]:
                        self.poutput("This command is not available in characterization mode.")
                        return

                return func(self, *args, **kwargs)
            return wrapper
        return decorator

    ###############################
    # MEASUREMENTS
    ###############################

    def _pedestal(self):
        
            self.poutput("Starting the acquisition of the electronic pedestal of the PMTs")
            self._pwr_off(channels="all")
            self._set_init_conf(channels="all", voltage_set=1200, threshold_set=100)
            time.sleep(0.1)
            self._rc_write(12, 1)
            time.sleep(0.1)
            self._acquire_charge(suffix="pedestal", flag_acq="pedestal", timer=120)
            time.sleep(0.1)
            self._rc_write(12, 0)
            time.sleep(0.1)
            if self.flag_test != 1:
                self._set_init_conf(channels="all", voltage_set=1200)
                time.sleep(0.1)
                self._pwr_on(channels="all")
            else:
                self.poutput("Pedestal acquisition in test environment fineshed")

        
    

    def _calib_polarizer(self, start_angle=0, step=5, ampl=110, near_w=10, far_w=6, voltage_ch=1200, 
                         time_acq=30, run_id = "pol", delay_win=0, width_win=130):
        """Function to calibrate the polarizer"""

        self._init_wheels(near_w, far_w)
        self._set_voltage(channels="all", voltage=voltage_ch)
        time.sleep(0.1)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)
        for i in range(start_angle, start_angle+ampl, step):
            try:
                self._init_polarizer(i)
                time.sleep(0.1)
                self._acquire_charge(suffix=str(i), timer=time_acq, flag_acq = "polarizer", run_id=run_id)
                time.sleep(0.1)

            except Exception as e:
                self.poutput(f"Problem occured during the calibration of the polarizer: {e}")
   
            self._rc_write(15, 0)
            time.sleep(0.1)
            self._rc_write(18, 0)
            time.sleep(0.1)
            self._rc_write(16, 0)
            time.sleep(0.1)

    

    def _spe_pmt(self, pol_angle = 50, near_w = 6, far_w = 10, voltage_ch = 1200, 
                 time_acq = 60, run_id="spe", delay_win=0, width_win=130):
        """Fnction to acquire SPE spectrum for PMTs"""

        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)
        self._set_voltage(channels="all", voltage=voltage_ch)
        time.sleep(0.1)
        try: 
            self._acquire_charge(suffix=str(voltage_ch), timer=time_acq, flag_acq = "spe", run_id=run_id)
        except Exception as e:
            self.poutput(f"Problem occured during the measurement of the spe: {e}")

        self._rc_write(15, 0)
        time.sleep(0.1)
        self._rc_write(18, 0)
        time.sleep(0.1)
        self._rc_write(16, 0)
        time.sleep(0.1)


    
    def _gain_pmt(self, pol_angle = 50, near_w = 6, far_w = 8, volt_start = 800, volt_end = 1400, deltav = 50,
                  time_acq = 30, run_id = "gain", delay_win=0, width_win=130):
        """Function to acquire gain spectrum from PMTs"""

        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)

        for volt in range(volt_start, volt_end+deltav, deltav): 

            self._set_voltage(channels="all", voltage=volt)
            self.poutput(f"Setted the voltage of the channels to the following value: {volt}")
            time.sleep(0.1)
            try: 
                self._acquire_charge(suffix=str(volt), timer=time_acq, flag_acq="gain", run_id = run_id)

            except Exception as e:
                self.poutput(f"Problem occurred during the gain measurement: {e}")


        self._rc_write(15, 0)
        time.sleep(0.1)
        self._rc_write(18, 0)
        time.sleep(0.1)
        self._rc_write(16, 0)
        time.sleep(0.1)

    
    def _wheels_characterisation(self, pol_angle = 30, near_start = 7, far_start = 8, voltage_ch = 1200, 
                                 time_acq=30, run_id = "char_wheels_pol_30", delay_win=0, width_win=130):
        

        self._set_voltage(channels="all", voltage=voltage_ch)
        time.sleep(0.1)
        self._init_polarizer(pol_angle)
        time.sleep(0.1) 
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)

        if self._check_signal():
            for i in range(near_start, 13):
                for j in range(far_start, 13):
                    self._init_wheels(i, j)
                    time.sleep(0.1)
                    try:
                        self._acquire_charge(suffix = f"wheels_{i}_{j}", flag_acq="wheels_char", run_id=run_id, timer=time_acq)
                    except Exception as e:
                        self.poutput(f"Problem occurred during the wheels characterisation: {e}")


            self._rc_write(15, 0)
            time.sleep(0.1)
            self._rc_write(18, 0)
            time.sleep(0.1)
            self._rc_write(16, 0)
            time.sleep(0.1)





    def _threshold_calibration(self, pol_angle, near_w, far_w,
                        voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6,
                        threshold_start, threshold_end, threshold_step,
                        time_acq=60, run_id="threshold_calibration", delay_win=0, width_win=130):
        
        """
        Performs a threshold scan by varying the threshold value and acquiring charge data for each step.
        This function is supposed to be used as long as the PMTs have been equalised.
        
        Parameters:
        pol_angle: Polarizer position angle.
        near_w, far_w: Positions for the near and far wheels.
        voltage_0, ..., voltage_6: Voltage values for each of the 7 channels.
        threshold_start: Initial threshold value.
        threshold_end: Final threshold value.
        threshold_step: Step value to change the threshold.
        time_acq: Acquisition time for each threshold value (in seconds).
        run_id: Identifier for the acquisition run.
        """

        self.poutput(f"Starting threshold scan (run_id={run_id}, acquisition time={time_acq}s)")
        

        
        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)
        voltages = [voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6]
        for channel in range(7):
            self._set_voltage(channels=str(1+channel), voltage=voltages[channel])
            time.sleep(0.1)

        for thr in range(threshold_start, threshold_end + threshold_step, threshold_step): 
            self._set_threshold(channels="all", threshold=thr)
            self.poutput(f"Set threshold to: {thr}")
            time.sleep(0.1)


            try: 
                self._acquire_charge(suffix=str(thr), timer=time_acq, flag_acq = "threshold", run_id=run_id)
                self.poutput(f"Acquisition complete for threshold {thr}")
            except Exception as e:
                self.poutput(f"Problem occured during the threshold calibration measurement: {e}")
        

        self._rc_write(15, 0)
        time.sleep(0.1)
        self._rc_write(18, 0)
        time.sleep(0.1)
        self._rc_write(16, 0)
        time.sleep(0.1)

        self.poutput("Threshold calibration completed.")






    def _equal_gain_spe(self, pol_angle, near_w, far_w, 
                        voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6,
                        threshold_0, threshold_1, threshold_2, threshold_3, threshold_4, threshold_5, threshold_6,
                        time_acq, run_id="equal_spe", delay_win=0, width_win=130):
        
        """Function to acquire SPE spectrum at equal gains for PMTs"""

        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, delay_win)
        time.sleep(0.1)
        self._rc_write(16, width_win)
        time.sleep(0.1)
        voltages = [voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6]
        thresholds = [threshold_0, threshold_1, threshold_2, threshold_3, threshold_4, threshold_5, threshold_6,]
        for channel in range(7):
            self._set_voltage(channels=str(1+channel), voltage=voltages[channel])
            self._set_threshold(channels=str(1+channel), threshold=thresholds[channel])
            time.sleep(0.1)

        try: 
            self._acquire_charge(suffix=str("spe_equal_gains"), timer=time_acq, flag_acq = "spe_equal", run_id=run_id)
        except Exception as e:
            self.poutput(f"Problem occured during the measurement of the spe: {e}")

        self._rc_write(15, 0)
        time.sleep(0.1)
        self._rc_write(18, 0)
        time.sleep(0.1)
        self._rc_write(16, 0)
        time.sleep(0.1)
    

    ########################
    #multiPMT COMMANDS
    ########################


    def _multi_threshold_calibration(self, flag_trg,
                        voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6,
                        threshold_start, threshold_end, threshold_step,
                        time_acq=60, run_id="threshold_calibration", delay_win=0, width_win=130):
        
        """
        Performs a threshold scan by varying the threshold value and acquiring charge data for each step.
        This function is supposed to be used as long as the PMTs have been equalised.
        
        Parameters:
        voltage_0, ..., voltage_6: Voltage values for each of the 7 channels.
        threshold_start: Initial threshold value.
        threshold_end: Final threshold value.
        threshold_step: Step value to change the threshold.
        time_acq: Acquisition time for each threshold value (in seconds).
        run_id: Identifier for the acquisition run.
        """

        self.poutput(f"Starting threshold scan (run_id={run_id}, acquisition time={time_acq}s)")
        

        if flag_trg:
            self._rc_write(15, 2)
            time.sleep(0.1)
            self._rc_write(18, delay_win)
            time.sleep(0.1)
            self._rc_write(16, width_win)
            time.sleep(0.1)


        voltages = [voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6]
        for channel in range(7):
            self._set_voltage(channels=str(1+channel), voltage=voltages[channel])
            time.sleep(0.1)

        for thr in range(threshold_start, threshold_end + threshold_step, threshold_step): 
            self._set_threshold(channels="all", threshold=thr)
            self.poutput(f"Set threshold to: {thr}")
            time.sleep(0.1)


            try: 
                self._acquire_charge(suffix=str(thr), timer=time_acq, flag_acq = "threshold", run_id=run_id)
                self.poutput(f"Acquisition complete for threshold {thr}")
            except Exception as e:
                self.poutput(f"Problem occured during the threshold calibration measurement: {e}")
        
        if flag_trg:
            self._rc_write(15, 0)
            time.sleep(0.1)
            self._rc_write(18, 0)
            time.sleep(0.1)
            self._rc_write(16, 0)
            time.sleep(0.1)

        self.poutput("Threshold calibration completed.")
    

    def _multi_equal_gain_spe(self, flag_trg, 
                        voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6,
                        threshold_0, threshold_1, threshold_2, threshold_3, threshold_4, threshold_5, threshold_6,
                        time_acq, run_id="equal_spe", delay_win=0, width_win=130):
        
        """Function to acquire SPE spectrum at equal gains for PMTs"""

        if flag_trg:
            self._rc_write(15, 2)
            time.sleep(0.1)
            self._rc_write(18, delay_win)
            time.sleep(0.1)
            self._rc_write(16, width_win)
            time.sleep(0.1)

        voltages = [voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6]
        thresholds = [threshold_0, threshold_1, threshold_2, threshold_3, threshold_4, threshold_5, threshold_6,]
        for channel in range(7):
            self._set_voltage(channels=str(1+channel), voltage=voltages[channel])
            self._set_threshold(channels=str(1+channel), threshold=thresholds[channel])
            time.sleep(0.1)

        try: 
            self._acquire_charge(suffix=str("spe_equal_gains"), timer=time_acq, flag_acq = "spe_equal", run_id=run_id)
        except Exception as e:
            self.poutput(f"Problem occured during the measurement of the spe: {e}")

        if flag_trg:
            self._rc_write(15, 0)
            time.sleep(0.1)
            self._rc_write(18, 0)
            time.sleep(0.1)
            self._rc_write(16, 0)
            time.sleep(0.1)
    

    def _multi_threshold_scan(self, flag_trg,
                        voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6,
                        threshold_start, threshold_end, threshold_step,
                        frequency, time_acq=60, run_id="threshold_scan",
                        registers="20,21,22,23,24,25,26",
                        delay_win=0, width_win=130
                        ):

        self.poutput(f"Starting threshold scan: run_id={run_id}, time_acq={time_acq}, frequency = {frequency}")

        self._rc_write(19, 127)
            
        time.sleep(0.1)
        

        if flag_trg:
            self._rc_write(15, 2)
            time.sleep(0.1)
            self._rc_write(18, delay_win)
            time.sleep(0.1)
            self._rc_write(16, width_win)
            time.sleep(0.1)

        voltages = [voltage_0, voltage_1, voltage_2, voltage_3, voltage_4, voltage_5, voltage_6]
        for channel in range(7):
            self._set_voltage(channels=str(1+channel), voltage=voltages[channel])
            time.sleep(0.1)

        for thr in range(threshold_start, threshold_end + threshold_step, threshold_step): 
            self._set_threshold(channels="all", threshold=thr)
            self.poutput(f"Set threshold to: {thr}")
            time.sleep(0.1)

            start_time = time.time()

            try:
                while time.time() - start_time < time_acq:
                    loop_start = time.time()
                    try:
                        self._acquire_rate(registers, flag_acq = "threshold_scan", suffix = thr, run_id = run_id)
                    except Exception as e:  
                        error_message = f"Error while acquiring data: {e}"
                        logging.error(error_message)
                        self.poutput(error_message)
                        break



                    delta = frequency - (time.time() - loop_start)
                    time.sleep(max(1, delta))
            
                for client in self.clients_connected:
                    for data_flag in ["rc_data", "hv_data", "mon_data"]:
                        MonitoringProcessing.CloseOpenFile(client=client, data_flag=data_flag, suffix=str(thr))




            except Exception as e:
                error_message = f"Problem occurred during the threshold scan: {e}"
                logging.error(error_message)
                self.poutput(error_message)
        

        if flag_trg:
            self._rc_write(15, 0)
            time.sleep(0.1)
            self._rc_write(18, 0)
            time.sleep(0.1)
            self._rc_write(16, 0)
            time.sleep(0.1)
    
        self._rc_write(19, 0)
            
        time.sleep(0.1)

        self.poutput("Threshold scan completed.")




    ##########################################
    # TERMINAL COMMANDS
    ##########################################
    client_parser = argparse.ArgumentParser()
    client_parser.add_argument("num_clients", type=str, help="The number of clients expected to connect")
    client_parser.add_argument("batch", type=int, help="Selects the BATCH of PMTs under test")
    client_parser.add_argument("main_id", type=str, help="Selects the id of the main connected to (example: cile1, milano, rio)")
    client_parser.add_argument("flag_acq_multi", type=int, help="Selects 1 if you are acquiring with a multiPMT or 0 if you are testing PMTs")
    client_parser.add_argument("--flag_test", type=int, help="Use 0 if the HV Boards are connected otherwise select 1", default=0)
    client_parser.add_argument("--port", type=int, help="Selects the port to establish the connection", default=DISCOVERY_PORT)

    @cmd2.with_argparser(client_parser)
    @cmd2.with_category("Clients Selection")
    def do_connect(self, args: argparse.Namespace):
        """
        Select a specific client multiPMT and verify the connection with the client itself.
        Usage: connect <client_ip>
        Example: connect 172.16.24.249
        """

        self._start_connection(args.port)
        if self._handshake(int(args.num_clients), args.flag_test):
            self.poutput(f"Connection with all the multiPMTs on port {args.port} was successful")
            self.prompt = f"|MultiPMT>"
            self.batch = args.batch
            self.flag_test = int(args.flag_test)
            self.main_id = str(args.main_id)
            self.flag_acq_multi = int(args.flag_acq_multi)
            if self.flag_acq_multi:
                try:
                    self._info_file_check()
                except Exception as e:
                    self.poutput(f"Info file checking had a problem {e}. Exiting connection...")
                    self._clean_up()
                    self.prompt = f"|Server>"
        else:
            self.poutput(f"Something went wrong during the handshake with the multiPMTs ")

    @cmd2.with_category("Generic Commands")
    def do_quit(self, _) -> None:
        """
        Quit from the application and restart client
        """
        if self.server:
            command_exit = {
                "type": "client_command",
                "command": "exit"
            }
            self._pwr_off(channels="all")
            time.sleep(0.1)
            self._rc_write(0, 0)
            time.sleep(0.1)
            self._rc_write(1, 0)
            time.sleep(0.1)
            for clients in self.clients_connected:
                self.server.send_multipart([clients, json.dumps(command_exit).encode("utf-8")])
        self.poutput("Quit command received. Shutting down...")
        self._clean_up()
        return super().do_quit(_)
    
    
    
    
    
    
    setup_parser = argparse.ArgumentParser()
    setup_subparsers = setup_parser.add_subparsers(title='subcommands', help='Setup subcommands')

    # Voltage
    voltage_parser = setup_subparsers.add_parser('voltage', help='PMTs voltages')
    voltage_parser.add_argument('values', nargs=CHANNELS, type=int,
                                help=f"Voltages for each of the {CHANNELS} channels")
    voltage_parser.set_defaults(func=partial(setup_voltage, subcmd='voltage'))

    # Very Low Threshold
    vlt_parser = setup_subparsers.add_parser('very_low_threshold', help='Very low thresholds')
    vlt_parser.add_argument('values', nargs=CHANNELS, type=int,
                            help=f"Very low thresholds for each of the {CHANNELS} channels")
    vlt_parser.set_defaults(func=partial(setup_very_low_threshold, subcmd='vlthr'))
    
    # Low Threshold
    lt_parser = setup_subparsers.add_parser('low_threshold', help='Low thresholds')
    lt_parser.add_argument('values', nargs=CHANNELS, type=int,
                            help=f"Low thresholds for each of the {CHANNELS} channels")
    lt_parser.set_defaults(func=partial(setup_low_threshold, subcmd='lthr'))

    # Medium Threshold
    mt_parser = setup_subparsers.add_parser('medium_threshold', help='Medium thresholds')
    mt_parser.add_argument('values', nargs=CHANNELS, type=int,
                            help=f"Medium thresholds for each of the {CHANNELS} channels")
    mt_parser.set_defaults(func=partial(setup_medium_threshold, subcmd='mthr'))
    
    # High Threshold
    ht_parser = setup_subparsers.add_parser('high_threshold', help='High thresholds')
    ht_parser.add_argument('values', nargs=CHANNELS, type=int,
                            help=f"High thresholds for each of the {CHANNELS} channels")
    ht_parser.set_defaults(func=partial(setup_high_threshold, subcmd='hthr'))

    # PMT Serial
    serial_parser = setup_subparsers.add_parser('pmt_serial', help='PMT serial numbers')
    serial_parser.add_argument('values', nargs=CHANNELS, type=str,
                                help=f"Serial number for each of the {CHANNELS} channels")
    serial_parser.set_defaults(func=partial(setup_pmt_serial, subcmd='serial'))

    
    @cmd2.with_argparser(setup_parser)
    @cmd2.with_category("Generic Commands")
    @command_guard('setup')
    def do_setup(self, args):
        func = getattr(args, 'func', None)
        if func is not None:
            if getattr(func, 'keywords', {}).get('subcmd') == 'serial':
                self.check_pmt_serials(args.values)  
            info = func(self, args)
            if info is not None:
                self.update_channel_info(info)
                
    
    @cmd2.with_category("Generic Commands")
    @command_guard('setup')
    def do_check_file(self,):
        self._info_file_check()
    
    ############
    # INSTRUMENTS
    ############

    wheels_parser = argparse.ArgumentParser()
    wheels_parser.add_argument("near_wheel_pos", type=int, help="Position of the near wheel")
    wheels_parser.add_argument("far_wheel_pos", type=int, help="Position of the far wheel")

    @cmd2.with_argparser(wheels_parser)
    @cmd2.with_category("Instruments")
    @command_guard('characterization')
    def do_wheels(self, args: argparse.Namespace):
        self._init_wheels(args.near_wheel_pos, args.far_wheel_pos)

    polarizer_parser = argparse.ArgumentParser()
    polarizer_parser.add_argument("pol_pos", type=float, help="Position of the polarizer")

    @cmd2.with_argparser(polarizer_parser)
    @cmd2.with_category("Instruments")
    @command_guard('characterization')
    def do_polarizer(self, args: argparse.Namespace):
        self._init_polarizer(args.pol_pos)

    ############
    # RC
    ############

    rc_write = argparse.ArgumentParser()
    rc_write.add_argument("rc_write_addr", type=int, help="The address of the register of the Run Control intended to be wrote")
    rc_write.add_argument("rc_write_value", type=int, help="The value intended to be wrote in the Run Control Register specified")

    @cmd2.with_argparser(rc_write)
    @cmd2.with_category("RC")
    @command_guard('safety_only')
    def do_write(self, args: argparse.Namespace) -> None:
        "Function to write user specified values in the Run Control registers"
        self._rc_write(args.rc_write_addr, args.rc_write_value)
    
    ############
    # HV
    ############

    hv_set_init_conf = argparse.ArgumentParser()
    hv_set_init_conf.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_set_init_conf.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")
    hv_set_init_conf.add_argument("--voltage_set", type=int, default=800, help="The default voltage to set (default: 800)")
    hv_set_init_conf.add_argument("--threshold_set", type=int, default=100, help="The threshold to set (default: 100)")
    hv_set_init_conf.add_argument("--limit_trip_time", type=int, default=2, help="The trip time limit (default: 2)")
    hv_set_init_conf.add_argument("--limit_voltage", type=int, default=100, help="The voltage limit (default: 100)")
    hv_set_init_conf.add_argument("--limit_current", type=int, default=5, help="The current limit (default: 5)")
    hv_set_init_conf.add_argument("--limit_temperature", type=int, default=50, help="The temperature limit (default: 50)")
    hv_set_init_conf.add_argument("--rate_up", type=int, default=25, help="The rate of voltage increase (default: 25)")
    hv_set_init_conf.add_argument("--rate_down", type=int, default=25, help="The rate of voltage decrease (default: 25)")

    @cmd2.with_argparser(hv_set_init_conf)
    @cmd2.with_category("HV")
    @command_guard('characterization')
    def do_set_init_conf(self, args: argparse.Namespace) -> None:
        "Function to set an initial configuration to the HV boards for the channel selected"
        self._set_init_conf(args.channels, args.port, args.voltage_set, args.threshold_set, args.limit_trip_time, args.limit_voltage, args.limit_current, args.limit_temperature, args.rate_up, args.rate_down)

    hv_set_voltage_set = argparse.ArgumentParser()
    hv_set_voltage_set.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_set_voltage_set.add_argument("voltage_set", type=int, help="The voltage to set")
    hv_set_voltage_set.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_set_voltage_set)
    @cmd2.with_category("HV")
    @command_guard('characterization')
    def do_set_voltage(self, args: argparse.Namespace) -> None:
        "Function to set the voltage set to the HV boards for the channels selected"
        self._set_voltage(args.channels, args.voltage_set, args.port)

    hv_on = argparse.ArgumentParser()
    hv_on.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_on.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_on)
    @cmd2.with_category("HV")
    @command_guard('characterization')
    def do_on(self, args: argparse.Namespace) -> None:
        "Function to power on all or selected channels"
        self._pwr_on(args.channels, args.port)


    hv_calib = argparse.ArgumentParser()
    hv_calib.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_calib.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_calib)
    @cmd2.with_category("HV")
    @command_guard('characterization')
    def do_hv_calibration(self, args: argparse.Namespace) -> None:
        "Function to calibrate all the HV boards connected"
        self._hv_calib(args.channels, args.port)

    
    # hv_startup = argparse.ArgumentParser()
    # hv_startup.add_argument("channels", type=str, help="The channels intended to be configured")
    # hv_startup.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    # @cmd2.with_argparser(hv_startup)
    # @cmd2.with_category("HV")
    # def do_hv_start_up(self, args: argparse.Namespace) -> None:
    #     "Function to calibrate all the HV boards connected"
    #     self._start_up(channels=args.channels, port=args.port)

    
    hv_prog = argparse.ArgumentParser()
    hv_prog.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_prog.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_prog)
    @cmd2.with_category("HV")
    @command_guard('safety_only')
    def do_feb_prog(self, args: argparse.Namespace) -> None:
        "Function to program all the FEBs boards connected"
        self._prog_feb(channels=args.channels, port=args.port)

    ############
    # DAQ
    ############

    daq_charge = argparse.ArgumentParser()
    daq_charge.add_argument("--timer", type=int, default=20, help="The time duration of the acquisition")
    daq_charge.add_argument("suffix", type=str, help="The suffix to put to characterize specific files")
    daq_charge.add_argument("flag", type=str, help="The flag of the acquisition type")
    daq_charge.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(daq_charge)
    @cmd2.with_category("DAQ")
    @command_guard('characterization')
    def do_acquire(self, args: argparse.Namespace) -> None:
        """Function to acquire the charges from the channels that are on"""
        self._acquire_charge(suffix=args.suffix, timer=args.timer, flag_acq=args.flag, run_id=args.run_id)

    ############
    # ACQ
    ############

    pol_parser = argparse.ArgumentParser()
    pol_parser.add_argument("start_angle", type=int, help="The initial angle of the polarizer")
    pol_parser.add_argument("step_angle", type=int, help="The step angle of the polarizer")
    pol_parser.add_argument("period_angle", type=int, help="The angle to determine the last value of the polarizer")
    pol_parser.add_argument("near_w", type=int, help="The position of the near wheel")
    pol_parser.add_argument("far_w", type=int, help="The position of the far wheel")
    pol_parser.add_argument("voltage_ch", type=int, help="The voltage of the channel")
    pol_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    pol_parser.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(pol_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_polarizer_acq(self, args: argparse.Namespace) -> None:
        self._calib_polarizer(args.start_angle, args.step_angle, args.period_angle, args.near_w, args.far_w, args.voltage_ch, args.timer_acq, args.run_id)


    pedestal_parser = argparse.ArgumentParser()

    @cmd2.with_argparser(pedestal_parser)
    @cmd2.with_category("ACQ")
    @command_guard('safety_only')
    def do_pedestal(self, args: argparse.Namespace) -> None:
        self._pedestal()



    spe_parser = argparse.ArgumentParser()
    spe_parser.add_argument("pol_angle", type=int, help="The angle of the polarizer")
    spe_parser.add_argument("near_w", type=int, help="The position of the near wheel")
    spe_parser.add_argument("far_w", type=int, help="The position of the far wheel")
    spe_parser.add_argument("voltage_ch", type=int, help="The voltage of the channel")
    spe_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    spe_parser.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(spe_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_spe_acq(self, args: argparse.Namespace) -> None:
        self._spe_pmt(args.pol_angle, args.near_w, args.far_w, args.voltage_ch, args.timer_acq, args.run_id)

    

    spe_equ_parser = argparse.ArgumentParser()
    spe_equ_parser.add_argument("pol_angle", type=int, help="The angle of the polarizer")
    spe_equ_parser.add_argument("near_w", type=int, help="The position of the near wheel")
    spe_equ_parser.add_argument("far_w", type=int, help="The position of the far wheel")
    spe_equ_parser.add_argument("voltage_0", type=int, help="The voltage of the channel 0")
    spe_equ_parser.add_argument("voltage_1", type=int, help="The voltage of the channel 1")
    spe_equ_parser.add_argument("voltage_2", type=int, help="The voltage of the channel 2")
    spe_equ_parser.add_argument("voltage_3", type=int, help="The voltage of the channel 3")
    spe_equ_parser.add_argument("voltage_4", type=int, help="The voltage of the channel 4")
    spe_equ_parser.add_argument("voltage_5", type=int, help="The voltage of the channel 5")
    spe_equ_parser.add_argument("voltage_6", type=int, help="The voltage of the channel 6")
    spe_equ_parser.add_argument("threshold_0", type=int, help="The threshold of the channel 0")
    spe_equ_parser.add_argument("threshold_1", type=int, help="The threshold of the channel 1")
    spe_equ_parser.add_argument("threshold_2", type=int, help="The threshold of the channel 2")
    spe_equ_parser.add_argument("threshold_3", type=int, help="The threshold of the channel 3")
    spe_equ_parser.add_argument("threshold_4", type=int, help="The threshold of the channel 4")
    spe_equ_parser.add_argument("threshold_5", type=int, help="The threshold of the channel 5")
    spe_equ_parser.add_argument("threshold_6", type=int, help="The threshold of the channel 6")
    spe_equ_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    spe_equ_parser.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(spe_equ_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_ch_spe_equ_acq(self, args: argparse.Namespace) -> None:
        self._equal_gain_spe(args.pol_angle, args.near_w, args.far_w,
                            args.voltage_0, args.voltage_1, args.voltage_2, args.voltage_3, args.voltage_4, args.voltage_5, args.voltage_6,
                            args.threshold_0, args.threshold_1, args.threshold_2, args.threshold_3, args.threshold_4, args.threshold_5, args.threshold_6,
                            args.timer_acq, args.run_id)


    
    gain_parser = argparse.ArgumentParser()
    gain_parser.add_argument("pol_angle", type=int, help="The angle of the polarizer")
    gain_parser.add_argument("near_w", type=int, help="The position of the near wheel")
    gain_parser.add_argument("far_w", type=int, help="The position of the far wheel")
    gain_parser.add_argument("voltage_start", type=int, help="The initial voltage value for the gain measurement")
    gain_parser.add_argument("voltage_end", type=int, help="The end voltage value for the gain measurement")
    gain_parser.add_argument("voltage_step", type=int, help="The step voltage for the gain measurement")
    gain_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    gain_parser.add_argument("run_id", type=str, help="The run id")


    @cmd2.with_argparser(gain_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_gain_acq(self, args: argparse.Namespace) -> None:
        self._gain_pmt(args.pol_angle, args.near_w, args.far_w, args.voltage_start, args.voltage_end, args.voltage_step, args.timer_acq, args.run_id)

    wheels_parser = argparse.ArgumentParser()
    wheels_parser.add_argument("pol_angle", type=int, help="The angle of the polarizer")
    wheels_parser.add_argument("near_start", type=int, help="The starting position of the near wheel")
    wheels_parser.add_argument("far_start", type=int, help="The starting position of the far wheel")
    wheels_parser.add_argument("voltage_channels", type=int, help="The voltage value for the wheels characterisation")
    wheels_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    wheels_parser.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(wheels_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_wheels_char(self, args: argparse.Namespace) -> None:
        self._wheels_characterisation(args.pol_angle, args.near_start, args.far_start, args.voltage_channels, args.timer_acq, args.run_id)


    thr_parser = argparse.ArgumentParser()
    thr_parser.add_argument("pol_angle", type=int, help="The angle of the polarizer")
    thr_parser.add_argument("near_w", type=int, help="The position of the near wheel")
    thr_parser.add_argument("far_w", type=int, help="The position of the far wheel")
    thr_parser.add_argument("voltage_0", type=int, help="The voltage of the channel 0")
    thr_parser.add_argument("voltage_1", type=int, help="The voltage of the channel 1")
    thr_parser.add_argument("voltage_2", type=int, help="The voltage of the channel 2")
    thr_parser.add_argument("voltage_3", type=int, help="The voltage of the channel 3")
    thr_parser.add_argument("voltage_4", type=int, help="The voltage of the channel 4")
    thr_parser.add_argument("voltage_5", type=int, help="The voltage of the channel 5")
    thr_parser.add_argument("voltage_6", type=int, help="The voltage of the channel 6")
    thr_parser.add_argument("threshold_start", type=int, help="The initial threshold value for the threshold scan measurement")
    thr_parser.add_argument("threshold_end", type=int, help="The end threshold value for the threshold scan measurement")
    thr_parser.add_argument("threshold_step", type=int, help="The step threshold value for the threshold scan measurement")
    thr_parser.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    thr_parser.add_argument("run_id", type=str, help="The run id")


    @cmd2.with_argparser(thr_parser)
    @cmd2.with_category("ACQ")
    @command_guard('characterization')
    def do_threshold_calibration(self, args: argparse.Namespace) -> None:
        self._threshold_calibration(pol_angle=args.pol_angle, near_w=args.near_w, far_w=args.far_w,
                            voltage_0=args.voltage_0, voltage_1=args.voltage_1, voltage_2=args.voltage_2,
                            voltage_3=args.voltage_3, voltage_4=args.voltage_4, voltage_5=args.voltage_5, voltage_6=args.voltage_6,
                            threshold_start=args.threshold_start, threshold_end=args.threshold_end, threshold_step=args.threshold_step,
                            time_acq=args.timer_acq, run_id=args.run_id)




    thr_parser_dark_multi = argparse.ArgumentParser()
    thr_parser_dark_multi.add_argument("voltage_0", type=int, help="The voltage of the channel 0")
    thr_parser_dark_multi.add_argument("voltage_1", type=int, help="The voltage of the channel 1")
    thr_parser_dark_multi.add_argument("voltage_2", type=int, help="The voltage of the channel 2")
    thr_parser_dark_multi.add_argument("voltage_3", type=int, help="The voltage of the channel 3")
    thr_parser_dark_multi.add_argument("voltage_4", type=int, help="The voltage of the channel 4")
    thr_parser_dark_multi.add_argument("voltage_5", type=int, help="The voltage of the channel 5")
    thr_parser_dark_multi.add_argument("voltage_6", type=int, help="The voltage of the channel 6")
    thr_parser_dark_multi.add_argument("flag_trg", type=int, help="Select 1 to enable the trigger acquisition mode, otherwise 0")
    thr_parser_dark_multi.add_argument("threshold_start", type=int, help="The initial threshold value for the threshold scan measurement")
    thr_parser_dark_multi.add_argument("threshold_end", type=int, help="The end threshold value for the threshold scan measurement")
    thr_parser_dark_multi.add_argument("threshold_step", type=int, help="The step threshold value for the threshold scan measurement")
    thr_parser_dark_multi.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    thr_parser_dark_multi.add_argument("run_id", type=str, help="The run id")


    @cmd2.with_argparser(thr_parser_dark_multi)
    @cmd2.with_category("ACQ")
    @command_guard('global_acq')
    def do_multi_threshold_calibration(self, args: argparse.Namespace) -> None:
        self._multi_threshold_calibration(
                            voltage_0=args.voltage_0, voltage_1=args.voltage_1, voltage_2=args.voltage_2,
                            voltage_3=args.voltage_3, voltage_4=args.voltage_4, voltage_5=args.voltage_5, voltage_6=args.voltage_6,
                            flag_trg=args.flag_trg,
                            threshold_start=args.threshold_start, threshold_end=args.threshold_end, threshold_step=args.threshold_step,
                            time_acq=args.timer_acq, run_id=args.run_id)
        
    


    spe_equ_parser_multi = argparse.ArgumentParser()
    spe_equ_parser_multi.add_argument("voltage_0", type=int, help="The voltage of the channel 0")
    spe_equ_parser_multi.add_argument("voltage_1", type=int, help="The voltage of the channel 1")
    spe_equ_parser_multi.add_argument("voltage_2", type=int, help="The voltage of the channel 2")
    spe_equ_parser_multi.add_argument("voltage_3", type=int, help="The voltage of the channel 3")
    spe_equ_parser_multi.add_argument("voltage_4", type=int, help="The voltage of the channel 4")
    spe_equ_parser_multi.add_argument("voltage_5", type=int, help="The voltage of the channel 5")
    spe_equ_parser_multi.add_argument("voltage_6", type=int, help="The voltage of the channel 6")
    spe_equ_parser_multi.add_argument("threshold_0", type=int, help="The threshold of the channel 0")
    spe_equ_parser_multi.add_argument("threshold_1", type=int, help="The threshold of the channel 1")
    spe_equ_parser_multi.add_argument("threshold_2", type=int, help="The threshold of the channel 2")
    spe_equ_parser_multi.add_argument("threshold_3", type=int, help="The threshold of the channel 3")
    spe_equ_parser_multi.add_argument("threshold_4", type=int, help="The threshold of the channel 4")
    spe_equ_parser_multi.add_argument("threshold_5", type=int, help="The threshold of the channel 5")
    spe_equ_parser_multi.add_argument("threshold_6", type=int, help="The threshold of the channel 6")
    spe_equ_parser_multi.add_argument("flag_trg", type=int, help="Select 1 to enable the trigger acquisition mode, otherwise 0")
    spe_equ_parser_multi.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    spe_equ_parser_multi.add_argument("run_id", type=str, help="The run id")

    @cmd2.with_argparser(spe_equ_parser_multi)
    @cmd2.with_category("ACQ")
    @command_guard('global_acq')
    def do_multi_spe_equ_acq(self, args: argparse.Namespace) -> None:
        self._multi_equal_gain_spe(flag_trg=args.flag_trg, 
                            voltage_0=args.voltage_0, voltage_1=args.voltage_1, voltage_2=args.voltage_2, voltage_3=args.voltage_3, voltage_4=args.voltage_4, voltage_5=args.voltage_5, voltage_6=args.voltage_6,
                            threshold_0=args.threshold_0, threshold_1=args.threshold_1, threshold_2=args.threshold_2, threshold_3=args.threshold_3, threshold_4=args.threshold_4, threshold_5=args.threshold_5, threshold_6=args.threshold_6,
                            time_acq=args.timer_acq, run_id=args.run_id)
    


    thr_scan_multi = argparse.ArgumentParser()
    thr_scan_multi.add_argument("voltage_0", type=int, help="The voltage of the channel 0")
    thr_scan_multi.add_argument("voltage_1", type=int, help="The voltage of the channel 1")
    thr_scan_multi.add_argument("voltage_2", type=int, help="The voltage of the channel 2")
    thr_scan_multi.add_argument("voltage_3", type=int, help="The voltage of the channel 3")
    thr_scan_multi.add_argument("voltage_4", type=int, help="The voltage of the channel 4")
    thr_scan_multi.add_argument("voltage_5", type=int, help="The voltage of the channel 5")
    thr_scan_multi.add_argument("voltage_6", type=int, help="The voltage of the channel 6")
    thr_scan_multi.add_argument("flag_trg", type=int, help="Select 1 to enable the trigger acquisition mode, otherwise 0")
    thr_scan_multi.add_argument("threshold_start", type=int, help="The initial threshold value for the threshold scan measurement")
    thr_scan_multi.add_argument("threshold_end", type=int, help="The end threshold value for the threshold scan measurement")
    thr_scan_multi.add_argument("threshold_step", type=int, help="The step threshold value for the threshold scan measurement")
    thr_scan_multi.add_argument("frequency", type=int, help="Frequency of measurement")
    thr_scan_multi.add_argument("--registers", type=str, help="registers to monitor", default="20,21,22,23,24,25,26")
    thr_scan_multi.add_argument("timer_acq", type=int, help="The timer of each acquisition")
    thr_scan_multi.add_argument("run_id", type=str, help="The run id")


    @cmd2.with_argparser(thr_scan_multi)
    @cmd2.with_category("ACQ")
    @command_guard('global_acq')
    def do_multi_threshold_scan(self, args: argparse.Namespace) -> None:
        self._multi_threshold_scan(flag_trg=args.flag_trg,
                            voltage_0=args.voltage_0, voltage_1=args.voltage_1, voltage_2=args.voltage_2,
                            voltage_3=args.voltage_3, voltage_4=args.voltage_4, voltage_5=args.voltage_5, voltage_6=args.voltage_6,
                            frequency=args.frequency, registers=args.registers,
                            threshold_start=args.threshold_start, threshold_end=args.threshold_end, threshold_step=args.threshold_step,
                            time_acq=args.timer_acq, run_id=args.run_id)
        

    
   



if __name__ == '__main__':

    context = zmq.Context()
    app = Server(context=context)
    try:
        app.cmdloop()
    except KeyboardInterrupt:
        app.poutput("\nShutting down...")
    finally:
        app._clean_up()
