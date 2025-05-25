import datetime
from bme280 import BME280
from tla2024 import TLA2024

IIC_BUS = 1
CHIP_ADDRESS_BME = 0x76
CHIP_ADDRESS_TLA = 0x49

bme = BME280(IIC_BUS, CHIP_ADDRESS_BME)
tla = TLA2024(IIC_BUS, CHIP_ADDRESS_TLA)



class MON:

    def __init__(self):
        pass

    def read_mon_data(self):

        data_bme = bme.readReg()
        data_tla = tla.readAll()

        timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')

        data = {}
        data["type"] = "data"
        data["data_type"] = "mon_data"
        data["board"] = {

            "time": timestamp,
            "temp": data_bme[0],
            "pressure": data_bme[1],
            "hum": data_bme[2],
            "5V": (data_tla[0]/1000) * 3,
            "3V3": (data_tla[2]/1000) * 2,
            "I": data_tla[1]
        }

        return data


