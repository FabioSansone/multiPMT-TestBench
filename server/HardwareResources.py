import logging
import json
import zmq
from pathlib import Path
from typing import List, Callable, Union
import time
import MonitoringProcessing
import datetime
import os
import subprocess
import ctypes

logger = logging.getLogger("Server")

###################
#FOLDER ACQUISITIO#
###################

FOLDER_ACQ = {
    "polarizer" : "polarizer_calibration/",
    "pedestal" : "pedestal_characterisation/",
    "spe" : "single_photoelectron/",
    "gain" : "gain_curve/",
    "wheels_char" : "wheels_characterisation/",
    "fiber_char" : "fiber_characterisation/",
    "threshold": "threshold_calibration/",
    "threshold_dark": "threshold_calibration_dark/",
    "threshold_scan": "threshold_scan/",
    "spe_equal": "spe_equal_gains/"
}

#####################################
#RUN CONTROL COMMUNICATION FUNCTIONS#
#####################################

def RCWrite(socket:zmq.Socket, clients: List[bytes], addr : int, value: int, output_func: Callable[[str], None]) -> None:
    """
    Sends an RC write command to connected clients.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used to send the command.
        clients (List[bytes]): The list of connected client IDs.
        addr (int): The address to write.
        value (int): The value to write.
        output_func (Callable[[str], None]): Function to output messages (e.g., poutput).

    Behavior:
        For each connected client, the function sends a JSON-encoded RC write command.
        It then waits for a response and, if the response indicates a successful RC write,
        outputs the result using the provided output function.
    """
    command_rc_write = {
            "type": "rc_command",
            "command": "write_address",
            "address": addr,
            "value": value
        }
    logger.info(f"Sending RC command to client: {command_rc_write}")

    for client in clients:
        socket.send_multipart([client, json.dumps(command_rc_write).encode("utf-8")])
        try:
            write = socket.recv_multipart()
            response = json.loads(write[1].decode("utf-8"))
            if write[0] == client and response.get("response") == "rc_write":
                output_func(response.get("result"))
        except Exception as e:
            output_func(f"Problem occured writing RC registers: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the RC response.")


def RCRead(socket:zmq.Socket, clients: List[bytes], addr : int, output_func: Callable[[str], None]) -> None:
    """
    Sends an RC read command to connected clients.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used to send the command.
        clients (List[bytes]): The list of connected client IDs.
        addr (int): The address to read.
        output_func (Callable[[str], None]): Function to output messages (e.g., poutput).

    Behavior:
        For each connected client, the function sends a JSON-encoded RC read command.
        It then waits for a response and, if the response indicates a successful RC red,
        outputs the result using the provided output function.
    """
    command_rc_read = {
            "type": "rc_command",
            "command": "read_address",
            "address": addr,
        }
    logger.info(f"Sending RC command to client: {command_rc_read}")

    for client in clients:
        socket.send_multipart([client, json.dumps(command_rc_read).encode("utf-8")])
        try:
            read = socket.recv_multipart()
            response = json.loads(read[1].decode("utf-8"))
            if read[0] == client and response.get("response") == "rc_read":
                value = response.get("result")
                if value:
                    output_func(f"It was possible to read the register {addr} with value ",value)
                    return value
                else:
                    output_func("It was not possible to read the selected register")
                    return value
        except Exception as e:
            output_func(f"Problem occured writing RC registers: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the RC response.")

def RCMonitoring(socket:zmq.Socket, clients: List[bytes], registers: Union[List[int], str], batch:int, flag_acq:str, suffix: str, run_id: str, output_func: Callable[[str], None]):

    command_rc_monitoring = {
        "type": "rc_command",
        "command": "rc_monitoring",
        "regs": registers,
    }
    logger.info(f"Sending RC command to client: {command_rc_monitoring}")

    for client in clients:
        socket.send_multipart([client, json.dumps(command_rc_monitoring).encode("utf-8")])
        try:
            mon = socket.recv_multipart()
            response = json.loads(mon[1].decode("utf-8"))
            if mon[0] == client and response.get("response") == "rc_mon":
                MonitoringProcessing.SaveDataCSV(client=client, data=response.get("result"), number=batch, flag_acq=flag_acq, suffix=suffix, run_id=run_id)
    
        except Exception as e:
            output_func(f"Problem occured acquiring RC registers: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the RC response.")
 





######################################
#HIGH VOLTAGE COMMUNICATION FUNCTIONS#
######################################

def HVSetInitConf(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], voltage_set:Union[int, None], threshold_set:int, 
                  limit_trip_time:int, limit_voltage:int, limit_current:int, limit_temperature:int, rate_up:int, rate_down:int,
                  output_func: Callable[[str], None] 
                  ) -> None:
    
    """
    Sends a high-voltage (HV) initialization configuration command to connected clients.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used for communication.
        clients (List[bytes]): The list of connected client IDs.
        port (str): The HV port to configure.
        channels (Union[List[str], str]): The channel(s) to configure.
        voltage_set (int): The voltage to set.
        threshold_set (int): The threshold to set.
        limit_trip_time (int): The trip time limit.
        limit_voltage (int): The voltage limit.
        limit_current (int): The current limit.
        limit_temperature (int): The temperature limit.
        rate_up (int): The ramp-up rate.
        rate_down (int): The ramp-down rate.
        output_func (Callable[[str], None]): Function to output messages.

    Behavior:
        Constructs and sends a JSON-encoded HV initialization configuration command to each client.
        It then waits for a response and outputs the channels for which the configuration was successful 
        and those for which it failed.
    """

    command_hv_init_conf = {
            "type": "hv_command",
            "command": "set_init_configuration",
            "port": port,
            "channel": channels,
            "voltage_set": voltage_set,
            "threshold_set": threshold_set,
            "limit_trip_time": limit_trip_time,
            "limit_voltage": limit_voltage,
            "limit_current": limit_current,
            "limit_temperature": limit_temperature,
            "rate_up": rate_up,
            "rate_down": rate_down
        }
    
    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_init_conf).encode("utf-8")])
        try:
            conf = socket.recv_multipart()
            response_conf = json.loads(conf[1].decode("utf-8"))
            if conf[0] == client and response_conf.get("response") == "hv_init_conf":
                output_func(f"It was possible to set the initial configuration for the following channels: {response_conf.get('result')[0]}. \n It was not possible to set the following channels: {response_conf.get('result')[1]}")
        except Exception as e:
            output_func(f"HV init conf problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the HV configuration response.")


def HVSetVoltage(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], voltage:int, output_func: Callable[[str], None]) -> None:

    """
    Sends a high-voltage command to set the voltage on specific channels.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used for communication.
        clients (List[bytes]): The list of connected client IDs.
        port (str): The HV port to use.
        channels (Union[List[str], str]): The channel(s) to configure.
        voltage (int): The voltage value to set.
        output_func (Callable[[str], None]): Function to output messages.

    Behavior:
        Sends a JSON-encoded command to set the voltage and waits for a response.
        Then outputs the result, indicating which channels have been successfully set.
    """

    command_hv_set_voltage = {
            "type": "hv_command",
            "command": "set_voltage",
            "port": port,
            "channel": channels,
            "voltage_set": voltage
        }
    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_set_voltage).encode("utf-8")])
        try:
            voltage_set = socket.recv_multipart()
            response_volt = json.loads(voltage_set[1].decode("utf-8"))
            if voltage_set[0] == client and response_volt.get("response") == "hv_voltage_set":
                output_func(f"It was possible to set the voltage for the following channels: {response_volt.get('result')[0]}. \n It was not possible to set the voltage for the following channels: {response_volt.get('result')[1]}")
        except Exception as e:
            output_func(f"HV set voltage problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the voltage set response.")

def HVSetThreshold(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], threshold:int, output_func: Callable[[str], None]) -> None:

    command_hv_set_threshold = {
        "type": "hv_command",
        "command": "set_threshold",
        "port": port,
        "channel": channels,
        "threshold_set": threshold
    }
    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_set_threshold).encode("utf-8")])
        try:
            threshold_set = socket.recv_multipart()
            response_threshold = json.loads(threshold_set[1].decode("utf-8"))
            if threshold_set[0] == client and response_threshold.get("response") == "hv_threshold_set":
                output_func(f"It was possible to set the threshold for the following channels: {response_threshold.get('result')[0]}. \n It was not possible to set the threshold for the following channels: {response_threshold.get('result')[1]}")
        except Exception as e:
            output_func(f"HV set threshold problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the threshold set response.")


def HVPowerOn(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], output_func: Callable[[str], None]) -> None:

    """
    Sends a high-voltage command to power on specific channels.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used for communication.
        clients (List[bytes]): The list of connected client IDs.
        port (str): The HV port to use.
        channels (Union[List[str], str]): The channel(s) to power on.
        output_func (Callable[[str], None]): Function to output messages.

    Behavior:
        Constructs and sends a JSON-encoded power-on command to each client.
        Outputs whether the power-on operation was successful based on the client response.
    """

    command_hv_on = {
            "type": "hv_command",
            "command": "set_power_on",
            "port": port,
            "channel": channels
        }
    
    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_on).encode("utf-8")])
        try:
            hv_on = socket.recv_multipart()
            response_on = json.loads(hv_on[1].decode("utf-8"))
            if hv_on[0] == client and response_on.get("result"):
               output_func("It was possible to power on all the channels selected")
            else:
                output_func("It was not possible to power on all the channels selected")
        except Exception as e:
            output_func(f"HV power on problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the power on response.")


def HVPowerOff(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], output_func: Callable[[str], None]) -> None:

    """
    Sends a high-voltage command to power off specific channels.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used for communication.
        clients (List[bytes]): The list of connected client IDs.
        port (str): The HV port to use.
        channels (Union[List[str], str]): The channel(s) to power off.
        output_func (Callable[[str], None]): Function to output messages.

    Behavior:
        Sends a JSON-encoded power-off command to each client.
        Waits for and processes the response, then outputs whether the operation was successful.
    """

    command_hv_on = {
            "type": "hv_command",
            "command": "set_power_off",
            "port": port,
            "channel": channels
        }
    
    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_on).encode("utf-8")])
        try:
            hv_on = socket.recv_multipart()
            response_on = json.loads(hv_on[1].decode("utf-8"))
            if hv_on[0] == client and response_on.get("result"):
                output_func("It was possible to power off all the channels selected")
            else:
                output_func("It was NOT possible to power off all the channels selected")
        except Exception as e:
            output_func(f"HV power off problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the power off response.")



def HVCalibration(socket:zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], output_func: Callable[[str], None]) -> None:

    """
    Sends a high-voltage calibration command to the specified channels.

    Parameters:
        socket (zmq.Socket): The ZMQ socket used for communication.
        clients (List[bytes]): The list of connected client IDs.
        port (str): The HV port to use.
        channels (Union[List[str], str]): The channel(s) to calibrate.
        output_func (Callable[[str], None]): Function to output messages.

    Behavior:
        Notifies the user that calibration is starting, sends a JSON-encoded calibration command,
        and then waits for the client response. The result (success or failure) is then outputted.
    """

    output_func("Starting the calibration of the channels selected. For more information on the status, check the client log")

    command_hv_calib = {
        "type": "hv_command",
        "command": "hv_calibration",
        "channels": channels,
        "port": port,

    }

    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_calib).encode("utf-8")])
        try:
            hv_calib = socket.recv_multipart()
            response_calib = json.loads(hv_calib[1].decode("utf-8"))
            if hv_calib[0] == client and response_calib.get("result"):
                output_func("It was possible to calibrate all the channels selected. See the client log for more details")
            else:
                output_func("It was not possible to calibrate all the channels selected. See the client log for more details")
        except Exception as e:
            output_func(f"HV calibration problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the calibration response.")


def HVGetSerialFEB(socket: zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], batch:int, output_func: Callable[[str], None]) -> None:

    output_func("Getting the serial numbers of the FEB and assoicating them with the corresponding channels")

    command_hv_serial = {
        "type": "hv_command",
        "command": "hv_serial",
        "channels": channels, 
        "port": port
    }

    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_serial).encode("utf-8")])
        try:
            hv_serial = socket.recv_multipart()
            response_serial = json.loads(hv_serial[1].decode("utf-8"))
            if hv_serial[0] == client and response_serial.get("response") == "hv_serial":
                #MonitoringProcessing.WriteSerialChannels(response_serial.get("result"), batch)
                output_func("Serial Numbers of the channels selected acquired and stored successfully")
                return response_serial.get("result")
            else:
                output_func("It was not possible to get the serial numbers of the channels selected. See the client log for more details")
                return None
        except Exception as e:
            output_func(f"HV Serial Number problem occured: {e}")
            return None
        except json.JSONDecodeError:
            output_func("Failed to decode the serial number response.")
            return None


def HVSetSerialPMT(socket: zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], serials:List[str], batch:int, output_func: Callable[[str], None]) -> None:

    output_func("Setting the serial numbers of the PMTs")

    command_hv_set_serial = {
        "type": "hv_command",
        "command": "set_serial",
        "channels": channels, 
        "port": port,
        "serials" : serials
    }

    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_set_serial).encode("utf-8")])
        try:
            hv_serial = socket.recv_multipart()
            response_serial = json.loads(hv_serial[1].decode("utf-8"))
            if hv_serial[0] == client and response_serial.get("response") == "set_serial":
                output_func("Serial Numbers of the channels set successfully")
                return response_serial.get("result")
            else:
                output_func("It was not possible to set the serial numbers of the channels selected. See the client log for more details")
                return None
        except Exception as e:
            output_func(f"HV Serial Number problem occured: {e}")
            return None
        except json.JSONDecodeError:
            output_func("Failed to decode the set serial number response.")
            return None
        
def HVProgFEB(socket: zmq.Socket, clients: List[bytes], port:str, channels:Union[List[str], str], baud: int, firmware:str, output_func: Callable[[str], None]) -> None:

    output_func("Programming the FEBs")

    command_hv_start_up = {
        "type": "hv_command",
        "command": "hv_prog_feb",
        "channels": channels, 
        "port": port,
        "baud" : baud,
        "firmware": firmware
    }


    for client in clients:
        socket.send_multipart([client, json.dumps(command_hv_start_up).encode("utf-8")])
        try:
            hv_prog = socket.recv_multipart()
            response_prog = json.loads(hv_prog[1].decode("utf-8"))
            if hv_prog[0] == client and response_prog.get("response") == "hv_start_up":
                output_func("It was possible to program all the FEBs")
            else:
                output_func("It was not possible to program all the FEBs selected. See the client log for more details")
        except Exception as e:
            output_func(f"HV Start Up problem occured: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the start up response.")

######################################
#DMA COMMUNICATION FUNCTIONS#
######################################

def GenerateTimestamp():
    return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')

def GenerateTimestampFolder():
    return datetime.datetime.now().strftime('%Y_%m_%d')

def GetFileName(suffix):
    timestamp = GenerateTimestamp()
    file_prefix = "daq"
    return f"{file_prefix}_{timestamp}_{suffix}.csv"

def CheckFileExist(fname):
        base, ext = os.path.splitext(fname)
        i = 1
        while os.path.exists(fname):
            fname = f"{base}_{i}{ext}"
            i += 1
        return fname

def GetFolderPath(flag_acq = "", run_id = None, number = None):
        base_path = Path("/swgo") if Path("/swgo").exists() else Path.home()
        base_folder = base_path / "multiPMT" / "acquisition" / f"batch_{number}" / FOLDER_ACQ.get(flag_acq, "unknown") / GenerateTimestampFolder()

        if run_id is not None:
            run_folder = base_folder / f"run_{run_id}"
        else:
            i = 1
            run_folder = base_folder / f"acq_{i}"
            while run_folder.exists():
                i += 1
                run_folder = base_folder / f"acq_{i}"

        run_folder.mkdir(parents=True, exist_ok=True)
        return run_folder


def CompileCLibrary(force_compile=False):
    build_dir = Path(__file__).parent / "../evreceiver"   # where is the .c
    source_file = build_dir / "evreceiver.c"            # C source
    output_lib = build_dir / "evreceiver.so"         # output for ctypes

    if not output_lib.exists() or force_compile:
        logger.warning("Compiling C shared library for ctypes...")

        compile_cmd = [
            "gcc",
            "-shared",
            "-fPIC",
            "-O2",
            str(source_file),
            "-o",
            str(output_lib),

            "-lzmq",
            "-lpthread"
        ]

        result = subprocess.run(compile_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Compilation failed:\n{result.stderr}")

        logger.warning("Compilation completed successfully.")

    return output_lib


def DMACommunication(socket:zmq.Socket, clients: List[bytes], suffix:str, flag_acquisition:str, run_id:Union[str, None], 
                     timer:int, batch:int, output_func: Callable[[str], None]) -> None:
    
    if timer is not None and timer < 10:
        logger.critical("Select a timer value greater than 10 seconds")
        return
    if timer is None:
        self.poutput("Acquisition will run forever. To stop press Ctrl-C")
        return
    
    ###Creating the folder###
    run_folder = GetFolderPath(flag_acq=flag_acquisition, run_id=run_id, number=batch)
    filename = CheckFileExist(GetFileName(suffix))
    filepath = run_folder / filename
    
    ###Compiling evreceiver###
    lib_path = CompileCLibrary(force_compile=False)
    c_lib = ctypes.CDLL(str(lib_path))

    ###Enabling the channels###
    RCWrite(socket=socket, clients=clients, addr=19, value=127, output_func=output_func)  
    time.sleep(0.1)

    ###Starting the Evproducer###
    c_lib.start_control.argtypes = []
    c_lib.start_control.restype = ctypes.c_int
    result_start = c_lib.start_control()
    if (result_start != 0):
        output_func("Problem in starting Evproducer. Check the log for more information")
        return 
    
    ###Starting the acquisition###
    c_lib.run.argtypes = [ctypes.c_int, ctypes.c_char_p]
    c_lib.run.restype = ctypes.c_int
    result_run = c_lib.run(timer, filepath.encode('utf-8'))
    if (result_run == 1):
        output_func("Acquisition time elapsed. Flushing last data...")
    elif(result_run == 0):
        output_func("Acquisition stopped by user. Flushing last data...")
    else:    
        output_func("An error occured durign the acquisition. Please check")
        return
    
    ###Disabling the channels###
    RCWrite(socket=socket, clients=clients, addr=19, value=0, output_func=output_func)  
    time.sleep(0.1)

    ###Flushing last data###
    read_prev_15 = RCRead(socket=socket, clients=clients, addr=15, output_func=output_func)
    time.sleep(0.1)
    if read_prev_15:
        RCWrite(socket=socket, clients=clients, addr=15, value=read_prev_15+32, output_func=output_func)
        time.sleep(0.1)
        read_now_15 = RCRead(socket=socket, clients=clients, addr=15, output_func=output_func)
        time.sleep(0.1)
        if (read_now_15-read_prev_15-32 == 64):
            output_func("Data flushing ended successfully")
            RCWrite(socket=socket, clients=clients, addr=15, value=read_prev_15, output_func=output_func)
            time.sleep(0.1)
        else:
            output_func("Problems occured durign the flushing of the last data. Please check")
            return
    
    ###Stopping evproducer###
    c_lib.stop_control.argtypes = []
    c_lib.stop_control.restype = ctypes.c_int
    result_stop = c_lib.stop_control()
    if (result_stop != 0):
        output_func("Problem in stopping Evproducer. Check the log for more information")
        return 


######################################
#MONITORING#
######################################


def Monitoring(socket: zmq.Socket, clients: List[bytes], rc_flag: int, hv_flag: int, mon_flag: int, batch: Union[int,str], flag_acq: str, suffix: str, run_id: str, output_func: Callable[[str], None]):

    command_monitoring = {
        "type": "mon_command",
        "command": "monitoring",
        "rc_flag": rc_flag,
        "hv_flag": hv_flag,
        "mon_flag": mon_flag,
    }
    logger.info(f"Sending RC command to client: {command_monitoring}")

    for client in clients:
        socket.send_multipart([client, json.dumps(command_monitoring).encode("utf-8")])
        try:
            mon_all = socket.recv_multipart()
            response = json.loads(mon_all[1].decode("utf-8"))
            if mon_all[0] == client and response.get("response") == "monitoring":
                mon_data = response.get("result")
                for i in mon_data:  
                    MonitoringProcessing.SaveDataCSV(client=client, data=i, number=batch, flag_acq=flag_acq, suffix=suffix, run_id=run_id)
        except Exception as e:
            output_func(f"Problem occured acquiring RC registers: {e}")
        except json.JSONDecodeError:
            output_func("Failed to decode the RC response.")

