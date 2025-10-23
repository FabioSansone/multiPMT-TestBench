import subprocess
import sys
import time
import logging
from types import SimpleNamespace

from rc_client import RC
from hv_client import HV


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
        result = subprocess.run(command, check=True)
        time.sleep(0.5)
        feb_logger.info("FEB flashed successfully")
        return True
    except subprocess.CalledProcessError as e:
        feb_logger.error(f"Flashing failed: {e.stderr}")
        return False
    except Exception as e:
        feb_logger.error(f"Unexpected error during flashing: {e}")
        return False


def changeAddress(index, rc, hv):
    """
    Set a new address for the FEB
    """
    try:
        
        if not rc.reset():
            feb_logger.error("Failed to reset Run Control")
            return False
        time.sleep(0.1)
        
        if not rc.write(1, addr_channels_encoding[index]):
            feb_logger.error(f"Failed to enable channel {index} via RC")
            return False
        time.sleep(0.1)
        
        
        hv.ResetConnection()
        time.sleep(2)
        
        
        feb_old_addr = hv.getStandardFebAddr()
        if feb_old_addr is None:
            feb_logger.error("No FEB found with standard address")
            return False
            
        new_address = index + 1
        
        if feb_old_addr == new_address:
            feb_logger.info(f"FEB already at correct address {new_address}")
            return True
            
        feb_logger.info(f"Changing FEB address from {feb_old_addr} to {new_address}")
        
        
        if not hv.checkAddress(feb_old_addr):
            feb_logger.error(f"Cannot connect to FEB at address {feb_old_addr}")
            return False
        
        
        feb_logger.info("Sending address change command...")
        try:
            hv.setModbusAddress(new_address)
        except Exception as e:
            feb_logger.error(f"Expected communication error during address change: {e}")
        
        
        
        hv.ResetConnection()
        time.sleep(2)
        
        
        if hv.checkAddress(new_address):
            feb_logger.info(f"FEB address successfully changed to {new_address}")
            return True
        else:
            feb_logger.error(f"Failed to verify new address {new_address}")
            return False

    except Exception as e:
        feb_logger.error(f"Unexpected error during address change: {e}")
        return False 



def main(channels, baud, firmware, port, rc, hv):
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
        
        success, valid_channels = rc.init_boot([channel])
        if not success:
            feb_logger.error(f"Failed to initialize channel {channel-1} in boot mode")
            continue
        
        time.sleep(0.1)
        
        if not boot(baud, firmware, port):
            feb_logger.error(f"Flashing failed for channel {channel-1}")
            continue
        
        time.sleep(1)
        
        channel_index = channel - 1
        if not changeAddress(channel_index, rc, hv):
            feb_logger.error(f"Address change failed for channel {channel}")
            continue
        
        successful_channels.append(channel)
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

