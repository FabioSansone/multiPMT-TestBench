import smbus2
import sys
import time

def conf_struct(os=0x01, mux=0x00, pga=0x02, mode=0x01, dr=0x04, reserved=0x00):
    """
    Costruisce il valore a 16 bit per il registro di configurazione del TLA2024.
    :param os: Operational Status o Start Single Conversion (1 bit)
    :param mux: Multiplexer configuration (3 bit)
    :param pga: Programmable Gain Amplifier (3 bit)
    :param mode: Operating Mode (1 bit)
    :param dr: Data Rate (3 bit)
    :param reserved: Riservato, impostato a 0 (5 bit)
    :return: Valore del registro di configurazione a 16 bit.
    """
    conf_msb = (
        (os & 0x01) << 7 |   # 1 bit per OS, posizione 15
        (mux & 0x07) << 4 |  # 3 bit per MUX, posizione 12-14
        (pga & 0x07) << 1 |   # 3 bit per PGA, posizione 9-11
        (mode & 0x01)    # 1 bit per MODE, posizione 8
    )

    conf_lsb = (
        (dr & 0x07) << 5  |   # 3 bit per DR, posizione 5-7
        (reserved & 0x1F)     # 5 bit per RESERVED, posizione 0-4
    )
    return [conf_msb, conf_lsb]


class TLA2024():

    def __init__(self, iic_bus, chip_addr):

        self.iic_bus = iic_bus
        self.chip_addr = chip_addr

        try:
            self.i2cbus = smbus2.SMBus(self.iic_bus)
        
        except IOError:
            print(f"E: I2C bus {iic_bus} not found")
            sys.exit(-1)

    CONFIGURATION_REGISTER = 0x01
    DATA_REGISTER = 0x00

    def read_conf_reg(self):
        conf = self.i2cbus.read_i2c_block_data(self.chip_addr, self.CONFIGURATION_REGISTER, 2) #I registri sono da 16 bit ma la funzione legge un byte alla volta
        return (conf[0] << 8) | conf[1]

    def write_conf_reg(self, data):
        self.i2cbus.write_i2c_block_data(self.chip_addr, self.CONFIGURATION_REGISTER, data)

    def read_data_reg(self):
        data = self.i2cbus.read_i2c_block_data(self.chip_addr, self.DATA_REGISTER, 2)
        return (data[0] << 8 | data[1]) >> 4 #Cosi' preservo il segno e mi tolgo i primi quattro bit riservati
    
    def isReady(self):
        ready = self.read_conf_reg() & 0x8000
        return ready
    
    def readAll(self):
        
        output = []
        mux = [0x04, 0x05, 0x06]
        fsr = [0x02, 0x02, 0x02]
        lsb = [1, 1, 1]
        os = 0x01
        mode = 0x01
        dr = 0x04
        reserved = 0x03


        for i in range(0,3):
            config = conf_struct(os=os, mux=mux[i], pga=fsr[i], mode=mode, dr=dr, reserved=reserved)
            self.write_conf_reg(config)

            while not self.isReady():
                pass
            
            output.append(self.read_data_reg()*lsb[i])

        return output
    
# def main():
#    tla = TLA2024(1, 0x49)
#    print(tla.readAll())

# if __name__=="__main__":
#   main()





