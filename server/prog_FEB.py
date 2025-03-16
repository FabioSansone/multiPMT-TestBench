from rc_exp import RC
import subprocess
import argparse
import time
import sys
from hvmodbus import HVModbus

def pars():
    parser = argparse.ArgumentParser()
    parser.add_argument("--firmware",  type= str, help="firmware version", default="HKL031V4B.hex")
    parser.add_argument("--baud",  type= str, help="baudrate", default="115200")
    parser.add_argument("--port",  type= str, help="port of the FEB", default="/dev/ttyPS1")
    parser.add_argument("--channels",  type= str, help="comma-separated list of channels connected to the FEB", default="all")



    return  parser.parse_args()


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

hv = HVModbus()
rc = RC()


def reset():
    """
    Reset function for the values of the register 0 and 1 of the Run Control
    """
    try:
        phase_1 = rc.write(0, 0)
        if phase_1 == 0:
            rc.write(1, 0)
            return True
        else:
            print("Something went wrong during the reset")
            return False
    except Exception as e:
        print(f"Something went wrong during the reset: {e}")
        return False

    
def init(value):
    """
    Write the same value to register 0 and 1 to open a specific channel in boot mode
    """
    try:
        reg_0 = rc.write(0, value)
        if reg_0 == 0:
            rc.write(17, value)
            rc.write(1, value)
            return 0
        else:
            print("Something went wrong during the initialisation of the channel")
            return -1
        
    except Exception as e:
        print(f"Something went wrong during the initialisation of the channel: {e}")
        return -1

 

def boot(baud, firmware, port):
    """
    Execute the booting command for the Front End Boards
    """
    command = ["stm32flash",  '-b', f'{baud}', '-w', f'{firmware}', '-e', '255',
                    '-v', f'{port}']
    try:
        subprocess.run(command, check=True)
        time.sleep(0.5)
    
    except Exception as e:
        print(f"Something went wrong programming the FEB: {e}")
        return -1

def get_standard_feb_addr(port):
    """
    Get the address FEB not setted by the user
    """
    for addr in range(1, 21):  
        probe = hv.probe(port, addr)
        if probe:
            print(f"FEB found at address {addr}")
            return addr  
    print("No FEB found in the address range 0-20.")
    return None  

def check_address(addr_hv):
      return (addr_hv >= 0 and addr_hv <= 20)

def check_address_change(port, new_addr):
    if (hv.open(port, new_addr)):
        print("The address has been changed correctly")
    else:
        print("Something went wrong during the change of the address")

def select(port, addr_feb):
    if(check_address(addr_feb)):
      if(hv.open(port, addr_feb)):
        return 0 
      else:
        print(f"HV module with address {addr_feb} not present")
        return -1
    else:
      print(f'E: modbus address outside boundary - min:0 max:20')


def change_addr(port, index):
    """
    Set a new address for the FEB
    """
    args = pars()
    try:
        reset()
        time.sleep(0.5)
        rc.write(1, addr_channels_encoding[index])
        time.sleep(0.5)
        try:
            check = get_standard_feb_addr(port)
            if check is None:
                print("It was not possible to change the address of the FEB")
                return False  
            elif check != index + 1:
                try:
                    select(port, check)
                    hv.setModbusAddress(index + 1)
                    print(args.firmware.split(".")[0][-2:])
                    hv.setFirmwareVersion(args.firmware.split(".")[0][-2:])
                    print(f"FEB setted to address {index+1}")
                    time.sleep(0.5)
                    try:
                        select(port, index + 1)
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"It was not possible to check for the change of the address: {e}")
                        return False
                except Exception as e:
                    print(f"Something went wrong changing the FEB address: {e}")
                    return False
            else:
                print(f"The FEB is already at address {index + 1}. Skipping...")
                return True  

        except Exception as e:
            print(f"It was not possible to open the FEB with the standard address in the change function: {e}")
            return False

    except Exception as e:
        print(f"Something went wrong during the address change: {e}")
        return False



def main():

    args = pars()


    if args.channels == "all":
        for i in range(0, 7):
            rst = reset()
            if rst:
                check = init(addr_channels_encoding[i])
                if check == 0:
                    print(f"Channel:{i}")
                    boot(args.baud, args.firmware, args.port)
                    time.sleep(1)
                    check_change = change_addr(args.port, i)
                    if check_change:
                        continue
                else:
                    print("Something went wrong during the initialisation of the channel")
            else:
                print("Something went wrong during the reset in the main function")

        reset()
    else:
        try:
            channel_list = [int(x) for x in args.channels.split(",")]
            for j in channel_list:
                rst = reset()
                if rst:
                    check = init(addr_channels_encoding[j])
                    if check == 0:
                        print(f"Channel:{j}")
                        boot(args.baud, args.firmware, args.port)
                        time.sleep(1)
                        check_change_2 = change_addr(args.port, j)
                        if check_change_2:
                            continue
                    else:
                        print("Something went wrong during the initialisation of the channel")
                else:
                    print("Something went wrong during the reset in the main")
            reset()

        except ValueError:
            print('E: failed to parse --channels - should be comma-separated list of integers')
            sys.exit(-1)



if __name__== "__main__":
    main()
