#!/usr/bin/env python3
#coding=utf-8
import cmd2
import zmq
import argparse
import logging
import json
import time
import HardwareResources
from InstrumentManager import InstrumentsManager
from data_processing import DataProcess


#Generic Constants
MAX_RETRIES = 3

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

#ZMQ Context Definition
context = zmq.Context()



class Server(cmd2.Cmd):
    "A terminal application to switch and interact with different multiPMT"

    intro = "Welcome to the control interface for the multiPMTs. Type ? or help to list commands."
    prompt = "|Server> "

    def __init__(self) -> None:
        super().__init__()
        self.server = None
        self.clients_connected = []  
        self.instrument_manager = InstrumentsManager(self.poutput)
        self.batch = None


    
    ##########################################
    # SERVER-CLIENT COMMUNICATION
    ##########################################

    def _start_connection(self, port = 8001):
        """Starts the connection with """
        try:
            self.server = context.socket(zmq.ROUTER)
            port = port #multiPMT_port[ip]
            self.server.bind(f"tcp://*:{port}")
            self.poutput(f"Server started on port {port}")
        except zmq.ZMQError as e:
            logger.error(f"Failed to bind socket on port {port}: {e}")
            self.poutput(f"Error: {e}")
            self.server = None

    def _handshake_attempt(self):

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
            socks = dict(poller.poll(POLLER_TIMEOUT_CONNECTION * 30)) #Wait 10 minutes to let the client set evproducer and the high voltage
            if self.server not in socks:
                self.poutput("Timeout in attesa della risposta EV.")
                return False

            ev_response = self.server.recv_multipart()  
            if ev_response[0] != client_id or ev_response[1] != b"EV Success":
                self.poutput("Handshake EV fallito.")
                return False
            
                   
            self.poutput("Everything has been set up")
            self.poutput(f"This is the list of the connected clients: {self.clients_connected}")
            return True
                    
        except zmq.ZMQError as e:
            self.poutput(f"ZMQ Error during handshake: {e}")
            return False
        except Exception as e:
            self.poutput(f"Unexpected error during handshake: {e}")
            return False
    
    def _handshake(self, num_clients):
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
                success = self._handshake_attempt()
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
        self.clients_connected.clear()
        if self.server:
            self.server.close()
        context.term()

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
        

    def _pwr_on(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVPowerOn(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)
        

    
    def _pwr_off(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVPowerOff(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)
        


    def _hv_calib(self, channels, port="/dev/ttyPS1"):
        HardwareResources.HVCalibration(socket=self.server, clients=self.clients_connected, port=port, channels=channels, output_func=self.poutput)

        


    ###############################
    # DAQ
    ###############################

    def _acquire_charge(self, suffix, flag_acq, run_id = None, timer=60):     
        charge = DataProcess()
        HardwareResources.DMACommunication(socket=self.server, clients=self.clients_connected, charge=charge, suffix=suffix, flag_acquisition=flag_acq, 
                                           run_id=run_id, timer=timer, batch=self.batch, output_func=self.poutput)





    ###############################
    # MEASUREMENTS
    ###############################

    def _pedestal(self):
        self.poutput("Starting the acquisition of the electronic pedestal of the PMTs")
        self._pwr_off(channels="all")
        time.sleep(0.1)
        self._rc_write(12, 1)
        time.sleep(0.1)
        self._acquire_charge(suffix="pedestal", flag_acq="pedestal", timer=120)
        time.sleep(0.1)
        self._rc_write(12, 0)
        time.sleep(0.1)
        self._set_init_conf(channels="all", voltage_set=1200)
        time.sleep(0.1)
        self._pwr_on(channels="all")

        
    

    def _calib_polarizer(self, start_angle=0, step=5, ampl=110, near_w=10, far_w=6, voltage_ch=1200, time_acq=30, run_id = "pol"):
        """Function to calibrate the polarizer"""
        self._init_wheels(near_w, far_w)
        self._set_voltage(channels="all", voltage=voltage_ch)
        time.sleep(0.1)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, 7250)
        time.sleep(0.1)
        self._rc_write(16, 400)
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

    

    def _spe_pmt(self, pol_angle = 50, near_w = 6, far_w = 10, voltage_ch = 1200, time_acq = 60, run_id="spe"):
        """Fnction to acquire SPE spectrum for PMTs"""
        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, 7250)
        time.sleep(0.1)
        self._rc_write(16, 400)
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


    
    def _gain_pmt(self, pol_angle = 50, near_w = 6, far_w = 8, volt_start = 800, volt_end = 1400, deltav = 50, time_acq = 30, run_id = "gain"):
        """Function to acquire gain spectrum from PMTs"""
        self._init_wheels(near_w, far_w)
        self._init_polarizer(pol_angle)
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, 7250)
        time.sleep(0.1)
        self._rc_write(16, 400)
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

    
    def _wheels_characterisation(self, pol_angle = 30, near_start = 7, far_start = 8, voltage_ch = 1200, time_acq=30, run_id = "char_wheels_pol_30"):

        self._set_voltage(channels="all", voltage=voltage_ch)
        time.sleep(0.1)
        self._init_polarizer(pol_angle)
        time.sleep(0.1) 
        self._rc_write(15, 2)
        time.sleep(0.1)
        self._rc_write(18, 7250)
        time.sleep(0.1)
        self._rc_write(16, 400)
        time.sleep(0.1)
        
        
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

    ##########################################
    # TERMINAL COMMANDS
    ##########################################
    client_parser = argparse.ArgumentParser()
    client_parser.add_argument("num_clients", type=str, help="The number of clients expected to connect")
    client_parser.add_argument("batch", type=int, help="Selects the BATCH of PMTs under test")
    client_parser.add_argument("--port", type=int, help="Selects the port to establish the connection", default=8001)

    @cmd2.with_argparser(client_parser)
    @cmd2.with_category("Clients Selection")
    def do_connect(self, args: argparse.Namespace):
        """
        Select a specific client multiPMT and verify the connection with the client itself.
        Usage: connect <client_ip>
        Example: connect 172.16.24.249
        """

        self._start_connection(args.port)
        if self._handshake(int(args.num_clients)):
            self.poutput(f"Connection with all the multiPMTs on port {args.port} was successful")
            self.prompt = f"|MultiPMT>"
            self.batch = args.batch
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
    
    ############
    # INSTRUMENTS
    ############

    wheels_parser = argparse.ArgumentParser()
    wheels_parser.add_argument("near_wheel_pos", type=int, help="Position of the near wheel")
    wheels_parser.add_argument("far_wheel_pos", type=int, help="Position of the far wheel")

    @cmd2.with_argparser(wheels_parser)
    @cmd2.with_category("Instruments")
    def do_wheels(self, args: argparse.Namespace):
        self._init_wheels(args.near_wheel_pos, args.far_wheel_pos)

    polarizer_parser = argparse.ArgumentParser()
    polarizer_parser.add_argument("pol_pos", type=float, help="Position of the polarizer")

    @cmd2.with_argparser(polarizer_parser)
    @cmd2.with_category("Instruments")
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
    def do_set_init_conf(self, args: argparse.Namespace) -> None:
        "Function to set an initial configuration to the HV boards for the channel selected"
        self._set_init_conf(args.channels, args.port, args.voltage_set, args.threshold_set, args.limit_trip_time, args.limit_voltage, args.limit_current, args.limit_temperature, args.rate_up, args.rate_down)

    hv_set_voltage_set = argparse.ArgumentParser()
    hv_set_voltage_set.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_set_voltage_set.add_argument("voltage_set", type=int, help="The voltage to set")
    hv_set_voltage_set.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_set_voltage_set)
    @cmd2.with_category("HV")
    def do_set_voltage(self, args: argparse.Namespace) -> None:
        "Function to set the voltage set to the HV boards for the channels selected"
        self._set_voltage(args.channels, args.voltage_set, args.port)

    hv_on = argparse.ArgumentParser()
    hv_on.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_on.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_on)
    @cmd2.with_category("HV")
    def do_on(self, args: argparse.Namespace) -> None:
        "Function to power on all or selected channels"
        self._pwr_on(args.channels, args.port)


    hv_calib = argparse.ArgumentParser()
    hv_calib.add_argument("channels", type=str, help="The channels intended to be configured")
    hv_calib.add_argument("--port", type=str, default="/dev/ttyPS1", help="The serial port used to communicate with the board")

    @cmd2.with_argparser(hv_calib)
    @cmd2.with_category("HV")
    def do_hv_calibration(self, args: argparse.Namespace) -> None:
        "Function to calibrate all the HV boards connected"
        self._hv_calib(args.channels, args.port)

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
    def do_polarizer_acq(self, args: argparse.Namespace) -> None:
        self._calib_polarizer(args.start_angle, args.step_angle, args.period_angle, args.near_w, args.far_w, args.voltage_ch, args.timer_acq, args.run_id)


    pedestal_parser = argparse.ArgumentParser()

    @cmd2.with_argparser(pedestal_parser)
    @cmd2.with_category("ACQ")
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
    def do_spe_acq(self, args: argparse.Namespace) -> None:
        self._spe_pmt(args.pol_angle, args.near_w, args.far_w, args.voltage_ch, args.timer_acq, args.run_id)


    
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
    def do_wheels_char(self, args: argparse.Namespace) -> None:
        self._wheels_characterisation(args.pol_angle, args.near_start, args.far_start, args.voltage_channels, args.timer_acq, args.run_id)



if __name__ == '__main__':
    app = Server()
    try:
        app.cmdloop()
    except KeyboardInterrupt:
        app.poutput("\nShutting down...")
    finally:
        app._clean_up()
