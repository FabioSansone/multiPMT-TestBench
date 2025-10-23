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
        print(f"Flashing FEB with firmware {firmware} on port {port}")
        result = subprocess.run(command, check=True)
        time.sleep(0.5)
        print("FEB flashed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Flashing failed: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error during flashing: {e}")
        return False


def changeAddress(index, rc, hv):
    """
    Set a new address for the FEB
    """
    try:
        
        if not rc.reset():
            print("Failed to reset Run Control")
            return False
        time.sleep(0.1)
        
        if not rc.write(1, addr_channels_encoding[index]):
            print(f"Failed to enable channel {index} via RC")
            return False
        time.sleep(0.1)
        
        
        hv.ResetConnection()
        time.sleep(0.5)
        
        
        feb_old_addr = hv.getStandardFebAddr()
        if feb_old_addr is None:
            print("No FEB found with standard address")
            return False
            
        new_address = index + 1
        
        if feb_old_addr == new_address:
            print(f"FEB already at correct address {new_address}")
            return True
            
        print(f"Changing FEB address from {feb_old_addr} to {new_address}")
        
        
        if not hv.checkAddress(feb_old_addr):
            print(f"Cannot connect to FEB at address {feb_old_addr}")
            return False
        
        
        print("Sending address change command...")
        try:
            hv.setModbusAddress(new_address)
        except Exception as e:
            print(f"Expected communication error during address change: {e}")
        
        
        print("Waiting for FEB reboot...")
        time.sleep(1)
        
        
        hv.ResetConnection()
        time.sleep(0.5)
        
        
        if hv.checkAddress(new_address):
            print(f"FEB address successfully changed to {new_address}")
            return True
        else:
            print(f"Failed to verify new address {new_address}")
            return False

    except Exception as e:
        print(f"Unexpected error during address change: {e}")
        return False 



def main(channels, baud, firmware, port, rc, hv):
    """
    Main function to program FEBs
    """
    print(f"Starting FEB programming: channels={channels}, baud={baud}, firmware={firmware}, port={port}")
    
    channels_list = hv.getChannels(channels)
    if not channels_list:
        print("No valid channels specified")
        return False
    
    print(f"Processing channels: {channels_list}")
    
    successful_channels = []
    
    for channel in channels_list:
        print(f"Programming channel {channel-1}")
        
        
        if not rc.reset():
            print(f"Failed to reset RC for channel {channel-1}")
            continue
        
        time.sleep(0.1)
        
        success, valid_channels = rc.init_boot([channel])
        if not success:
            print(f"Failed to initialize channel {channel-1} in boot mode")
            continue
        
        time.sleep(0.1)
        
        if not boot(baud, firmware, port):
            print(f"Flashing failed for channel {channel-1}")
            continue
        
        time.sleep(1)
        
        channel_index = channel - 1
        if not changeAddress(channel_index, rc, hv):
            print(f"Address change failed for channel {channel}")
            continue
        
        successful_channels.append(channel)
        print(f"Channel {channel-1} programmed successfully")
    
    if successful_channels:
        print(f"Setting programmed channels to data mode: {successful_channels}")
        success, final_channels = rc.init_data(successful_channels)
        if success:
            print(f"Successfully programmed {len(successful_channels)} channels: {successful_channels}")
            return True
        else:
            print("Failed to set channels to data mode")
            return False
    else:
        print("No channels were successfully programmed")
        return False


if __name__ == "__main__":

    
    
    # Configurazione di esempio
    channels = "2"  # o "1,2,3" o [1,2,3]
    baud = 115200
    firmware = "HKL031V4B.hex" 
    port = "/dev/ttyPS1"

    params = SimpleNamespace(mode = 'rtu',
                            host = 'localhost',
                            port = port)
    
    rc1 = RC()
    hv1 = HV(params=params)
    
    result = main(channels, baud, firmware, port, rc1, hv1)
    sys.exit(0 if result else 1)