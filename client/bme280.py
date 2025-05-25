import smbus2
import sys
from ctypes import c_short
import time

def unsigned_short(data, index):
    uword = (data[index+1] << 8) + data[index]
    return uword

def signed_short(data, index):
    sword = c_short((data[index+1] << 8) + data[index]).value
    return sword

def unsigned_char(data, index):
    result =  data[index] & 0xFF
    return result

def signed_char(data, index):

    result = data[index]
    if result > 127:
        result -= 256
    return result








class BME280():

    def __init__(self, iic_bus, chip_addr):

        self.iic_bus = iic_bus
        self.chip_addr = chip_addr

        try:
            self.i2cbus = smbus2.SMBus(self.iic_bus)
        
        except IOError:
            print(f"E: I2C bus {iic_bus} not found")
            sys.exit(-1)
    

    def readId(self):
      REG_ID = 0xD0
      chip_id = self.i2cbus.read_i2c_block_data(self.chip_addr, REG_ID, 1)
      return chip_id


    
    def readReg(self):

        REG_DATA = 0xF7 #Burst read from 0xF7 to 0xFE
        REG_CONTROL = 0xF4
        REG_CONTROL_HUM = 0xF2

        OVERSAMPLE_T = 2
        OVERSAMPLE_P = 2
        OVERSAMPLE_H = 2

        MODE = 1

        self.i2cbus.write_byte_data(self.chip_addr, REG_CONTROL_HUM, OVERSAMPLE_H)


        control = OVERSAMPLE_T<<5 | OVERSAMPLE_P<<2 | MODE
        self.i2cbus.write_byte_data(self.chip_addr, REG_CONTROL, control)



        calib1 = self.i2cbus.read_i2c_block_data(self.chip_addr, 0x88, 24)
        calib2 = self.i2cbus.read_i2c_block_data(self.chip_addr, 0xA1, 1)
        calib3 = self.i2cbus.read_i2c_block_data(self.chip_addr, 0xE1, 7)

        dig_T1 = unsigned_short(calib1, 0)
        dig_T2 = signed_short(calib1, 2)
        dig_T3 = signed_short(calib1, 4)

        dig_P1 = unsigned_short(calib1, 6)
        dig_P2 = signed_short(calib1, 8)
        dig_P3 = signed_short(calib1, 10)
        dig_P4 = signed_short(calib1, 12)
        dig_P5 = signed_short(calib1, 14)
        dig_P6 = signed_short(calib1, 16)
        dig_P7 = signed_short(calib1, 18)
        dig_P8 = signed_short(calib1, 20)
        dig_P9 = signed_short(calib1, 22)

        dig_H1 = unsigned_char(calib2, 0)
        dig_H2 = signed_short(calib3, 0)
        dig_H3 = unsigned_char(calib3, 2)
        dig_H4 = signed_char(calib3, 3)
        dig_H4 = dig_H4 << 4 | (signed_char(calib3, 4) & 0x0F)
        if dig_H4 & (1<<11):
            dig_H4 |= 0xF000

        dig_H5 = signed_char(calib3, 5)
        #shiftando a sinistra di 4 mantengo complessivamente il segno inalterato
        dig_H5 = dig_H5 << 4 | ((signed_char(calib3, 4) >> 4) & 0x0F)
        #Estendo il segno per avere rappresentazione a 16 bit
        if dig_H5 & (1<<11):
            dig_H5 |= 0xF000

        dig_H6 = signed_char(calib3, 6)


        wait_time = 1.25 + (2.3 * OVERSAMPLE_T) + (2.3 * OVERSAMPLE_P + 0.575) + (2.3 * OVERSAMPLE_H + 0.575)
        time.sleep(wait_time/1000)

        data = self.i2cbus.read_i2c_block_data(self.chip_addr, REG_DATA, 8)
        pres_raw = (data[0] << 12) | (data[1] << 4) | (data[2] >> 4)
        temp_raw = (data[3] << 12) | (data[4] << 4) | (data[5] >> 4)
        hum_raw = (data[6] << 8) | data[7]

        var1 = ((((temp_raw>>3)-(dig_T1<<1)))*(dig_T2)) >> 11
        var2 = (((((temp_raw>>4) - (dig_T1)) * ((temp_raw>>4) - (dig_T1))) >> 12) * (dig_T3)) >> 14
        t_fine = var1+var2
        temperature = float(((t_fine * 5) + 128) >> 8);


        var1 = t_fine / 2.0 - 64000.0
        var2 = var1 * var1 * dig_P6 / 32768.0
        var2 = var2 + var1 * dig_P5 * 2.0
        var2 = var2 / 4.0 + dig_P4 * 65536.0
        var1 = (dig_P3 * var1 * var1 / 524288.0 + dig_P2 * var1) / 524288.0
        var1 = (1.0 + var1 / 32768.0) * dig_P1
        if var1 == 0:
            pressure=0
        else:
            pressure = 1048576.0 - pres_raw
            pressure = ((pressure - var2 / 4096.0) * 6250.0) / var1
            var1 = dig_P9 * pressure * pressure / 2147483648.0
            var2 = pressure * dig_P8 / 32768.0
            pressure = pressure + (var1 + var2 + dig_P7) / 16.0


        humidity = t_fine - 76800.0
        humidity = (hum_raw - (dig_H4 * 64.0 + dig_H5 / 16384.0 * humidity)) * (dig_H2 / 65536.0 * (1.0 + dig_H6 / 67108864.0 * humidity * (1.0 + dig_H3 / 67108864.0 * humidity)))
        humidity = humidity * (1.0 - dig_H1 * humidity / 524288.0)
        if humidity > 100:
            humidity = 100
        elif humidity < 0:
            humidity = 0

        return temperature/100.0,pressure/100.0,humidity
        

#def main():
#   bme = BME280(1, 0x76)
#   chip_id = bme.readId()
#   print("Chip ID     :", chip_id)

#   while True:
#      temperature,pressure,humidity = bme.readReg()
#      print("Temperature : ", temperature, "C")
#      print("Pressure : ", pressure, "hPa")
#      print("Humidity : ", humidity, "%")
#      print('')
#      time.sleep(1)

#if __name__=="__main__":
#   main()









            


