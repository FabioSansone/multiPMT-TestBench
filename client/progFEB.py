import subprocess
import sys
import time
import logging

from rc_client import RC
from hv_client import HV

rc = RC()
hv = HV()

feb_logger = logging.getLogger("Client")

#Dizionario che serve conversione tra l'indice del canale in decimale e il suo indice codificato one-hot per il Run Control
addr_channels_encoding = {
    0 : 1,
    1 : 2, 
    2 : 4,
    3 : 8,
    4:  16,
    5 : 32,
    6 : 64,
}

def boot(baud, firmware, port):
    """
    Execute the booting command for the Front End Boards
    """
    command = ["stm32flash", '-b', f'{baud}', '-w', f'{firmware}', '-e', '255', '-v', f'{port}']
    
    try:
        feb_logger.info(f"Flashing FEB with firmware {firmware} on port {port}")
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        time.sleep(0.5)
        feb_logger.info("FEB flashed successfully")
        return True
    except subprocess.CalledProcessError as e:
        feb_logger.error(f"Flashing failed: {e.stderr}")
        return False
    except Exception as e:
        feb_logger.error(f"Unexpected error during flashing: {e}")
        return False


def changeAddress(index):
    """
    Set a new address for the FEB
    """

    try:
        rc.reset()
        time.sleep(0.1)
        rc.write(1, addr_channels_encoding[index])
        time.sleep(0.1)
        try:
            feb_old_addr = hv.getStandardFebAddr()
            if feb_old_addr is None:
                feb_logger.error("It was not possible to change the address of the FEB")
                return False
            elif feb_old_addr != index + 1:
                try:
                    hv.checkAddress(feb_old_addr)
                    hv.setModbusAddress(index+1)
                    feb_logger.info(f"FEB setted to address {index+1}")
                    time.sleep(0.5)
                    try:
                        hv.checkAddress(index+1)
                        time.sleep(0.5)
                    except Exception as e:
                        feb_logger.error(f"It was not possible to check for the change of the address: {e}")
                        return False
                    
                    return True
                except Exception as e:
                    feb_logger.error(f"Something went wrong changing the FEB address: {e}")
                    return False
            else:
                feb_logger.info(f"The FEB is already at address {index + 1}. Skipping...")
                return True

        except Exception as e:
            feb_logger.error(f"It was not possible to open the FEB with the standard address in the change function: {e}")
            return False

    except Exception as e:
        feb_logger.error(f"Something went wrong during the address change: {e}")
        return False    



def main(channels, baud, firmware, port):
    """
    Main function to program FEBs
    """
    feb_logger.info(f"Starting FEB programming: channels={channels}, baud={baud}, firmware={firmware}, port={port}")
    
    channels_list = hv.getChannels(channels)
    if not channels_list:
        feb_logger.error("No valid channels specified")
        return False
    
    feb_logger.info(f"Processing channels: {channels_list}")
    
    successful_channels = []
    
    for channel in channels_list:
        feb_logger.info(f"Programming channel {channel-1}")
        
        
        if not rc.reset():
            feb_logger.error(f"Failed to reset RC for channel {channel-1}")
            continue
        
        time.sleep(0.1)
        
        success, valid_channels = rc.init_boot([channel-1])
        if not success:
            feb_logger.error(f"Failed to initialize channel {channel-1} in boot mode")
            continue
        
        time.sleep(0.1)
        
        if not boot(baud, firmware, port):
            feb_logger.error(f"Flashing failed for channel {channel-1}")
            continue
        
        time.sleep(1)
        
        channel_index = channel - 1
        if not changeAddress(channel_index):
            feb_logger.error(f"Address change failed for channel {channel}")
            continue
        
        successful_channels.append(channel-1)
        feb_logger.info(f"Channel {channel-1} programmed successfully")
    
    if successful_channels:
        feb_logger.info(f"Setting programmed channels to data mode: {successful_channels}")
        success, final_channels = rc.init_data(successful_channels)
        if success:
            feb_logger.info(f"Successfully programmed {len(successful_channels)} channels: {successful_channels}")
            return True
        else:
            feb_logger.error("Failed to set channels to data mode")
            return False
    else:
        feb_logger.error("No channels were successfully programmed")
        return False


if __name__ == "__main__":
    # Configurazione di esempio
    channels = "all"  # o "1,2,3" o [1,2,3]
    baud = 115200
    firmware = "HKL031V4B.hex" 
    port = "/dev/ttyPS1"
    
    result = main(channels, baud, firmware, port)
    sys.exit(0 if result else 1)