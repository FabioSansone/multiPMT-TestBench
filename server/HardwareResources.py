import logging
import json
import zmq
from typing import List, Callable, Union
import data_processing
import time

logger = logging.getLogger("Server")

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
        socket.send_multipart([clients, json.dumps(command_hv_calib).encode("utf-8")])
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


######################################
#DMA COMMUNICATION FUNCTIONS#
######################################


def DMACommunication(socket:zmq.Socket, clients: List[bytes], charge:data_processing.DataProcess, suffix:str, flag_acquisition:str, run_id:Union[str, None], 
                     timer:int, batch:int, output_func: Callable[[str], None]) -> None:
    
    if timer is not None and timer < 10:
        logger.critical("Select a timer value greater than 10 seconds")
        return
    if timer is None:
        self.poutput("Timer has not been set. Choose a proper value for the acquisition.")
        return

    RCWrite(socket=socket, clients=clients, addr=19, value=127, output_func=output_func)  

    time.sleep(0.1)
    output_func("Waiting time to settle evproducer")
    time.sleep(2) #Waiting time to settle evproducer

    ######################
    if suffix != "pedestal":
        output_func("Checking signal integrity")
        try: 
            signal_status = charge.signal_integrity(duration=60)
            if not signal_status:
                output_func("Check the signal on the oscilloscope. Something is probably wrong")
                return
        except Exception as e:
            output_func(f"Some problems occured checking signal integrity:{e}")

        output_func("Checked signal intgrety. Waiting 2 seconds to start empting the FIFO")
        time.sleep(2)
    ######################

    ######################
    output_func("Removing old data in the FIFO (30 seconds wait)")
    try: 
        charge.flush_fifo(duration=30)
    except Exception as e:
        output_func(f"Some problems occured empting the FIFO:{e}")

    output_func("Emptied FIFO. Waiting 3 seconds to start the acquisition")
    time.sleep(3)
    ######################

    output_func(f"Acquisition started. Waiting for {timer} seconds.")
    try: 
        charge.run(duration=timer, suffix=suffix, flag_acq=flag_acquisition, run_id=run_id, number = batch)
    except Exception as e:
        output_func(f"Some problems occured starting or managing the acquisition:{e}")

    output_func("Acquisition time has expired")

    time.sleep(0.1)
    RCWrite(socket=socket, clients=clients, addr=19, value=0, output_func=output_func)  


