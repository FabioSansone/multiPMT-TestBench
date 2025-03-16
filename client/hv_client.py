import minimalmodbus
import time
import datetime
import struct
import numpy as np
import logging

logger_hv = logging.getLogger("Client")

class HV():

    def __init__(self) -> None:
        
        self.dev = None
        self.address = None
        self.maxAddress = 7

    def probe(self, serial, addr):
        dev = minimalmodbus.Instrument(serial, addr)
        dev.serial.baudrate = 115200
        dev.serial.timeout = 0.5
        dev.mode = minimalmodbus.MODE_RTU

        found = False
        for _ in range(0, 3):
            try:
                dev.read_register(0x00)  # read modbus address register
                found = True
                break
            except IOError:
                pass

        return found

    def open(self, serial, addr): #Serial corresponds to the port and addr to the channel
        if self.probe(serial, addr):
            self.dev = minimalmodbus.Instrument(serial, addr)
            self.dev.serial.baudrate = 115200
            self.dev.serial.timeout = 0.5
            self.dev.mode = minimalmodbus.MODE_RTU
            self.address = addr
            return True
        else:
            return False
        
    def checkAddressBoundary(self, channel):
        return channel >= 1 and channel <= 20
    
    def isConnected(self):
        return self.address is not None

    def getAddress(self):
        return self.address
    

    def getStatus(self):
        return self.dev.read_register(0x0006)

    def getVoltage(self):
        lsb = self.dev.read_register(0x002A)
        msb = self.dev.read_register(0x002B)
        value = (msb << 16) + lsb
        return value / 1000

    def getVoltageSet(self):
        return self.dev.read_register(0x0026)

    def setVoltageSet(self, value):
        self.dev.write_register(0x0026, value)

    def getCurrent(self):
        lsb = self.dev.read_register(0x0028)
        msb = self.dev.read_register(0x0029)
        value = (msb << 16) + lsb
        return value / 1000

    def getTemperature(self):
        return self.dev.read_register(0x0007)

    def getRate(self, fmt=str):
        rup = self.dev.read_register(0x0023)
        rdn = self.dev.read_register(0x0024)
        if fmt == str:
            return f'{rup}/{rdn}'
        else:
            return rup, rdn

    def setRateRampup(self, value):
        self.dev.write_register(0x0023, value, functioncode=6)

    def setRateRampdown(self, value):
        self.dev.write_register(0x0024, value)

    def getLimit(self, fmt=str):
        lv = self.dev.read_register(0x0027)
        li = self.dev.read_register(0x0025)
        lt = self.dev.read_register(0x002F)
        ltt = self.dev.read_register(0x0022)
        if fmt == str:
            return f'{lv}/{li}/{lt}/{ltt}'
        else:
            return lv, li, lt, ltt

    def setLimitVoltage(self, value):
        self.dev.write_register(0x0027, value)

    def setLimitCurrent(self, value):
        self.dev.write_register(0x0025, value)

    def setLimitTemperature(self, value):
        self.dev.write_register(0x002F, value)

    def setLimitTriptime(self, value):
        self.dev.write_register(0x0022, value)

    def setThreshold(self, value):
        self.dev.write_register(0x002D, value)

    def getThreshold(self):
        return self.dev.read_register(0x002D)

    def getAlarm(self):
        return self.dev.read_register(0x002E)

    def getVref(self):
        return self.dev.read_register(0x002C) / 10

    def powerOn(self):
        self.dev.write_bit(1, True)

    def powerOff(self):
        self.dev.write_bit(1, False)

    def reset(self):
        self.dev.write_bit(2, True)
    
    def convert_temp(self, t):
        quoz = (t & 0xFF) / 1000.
        integer = (t >> 8) & 0xFF
        return round(integer + quoz, 2)

    def getInfo(self):
        fwver = self.dev.read_string(0x0002, 1)
        pmtsn = self.dev.read_string(0x0008, 6)
        hvsn = self.dev.read_string(0x000E, 6)
        febsn = self.dev.read_string(0x0014, 6)
        dev_id = self.dev.read_registers(0x004, 2)
        return fwver, pmtsn, hvsn, febsn, (dev_id[1] << 16) + dev_id[0]

    def readMonRegisters(self):
        monData = {}
        baseAddress = 0x0000
        regs = self.dev.read_registers(baseAddress, 48)
        monData['status'] = regs[0x0006]
        monData['Vset'] = regs[0x0026]
        monData['V'] = ((regs[0x002B] << 16) + regs[0x002A]) / 1000
        monData['I'] = ((regs[0x0029] << 16) + regs[0x0028]) / 1000
        monData['T'] = self.convert_temp(regs[0x0007])
        monData['rateUP'] = regs[0x0023]
        monData['rateDN'] = regs[0x0024]
        monData['limitV'] = regs[0x0027]
        monData['limitI'] = regs[0x0025]
        monData['limitT'] = regs[0x002F]
        monData['limitTRIP'] = regs[0x0022]
        monData['threshold'] = regs[0x002D]
        monData['alarm'] = regs[0x002E]
        return monData
    

    def check_address(self, port, channel):
        if self.open(port, channel):
            if self.getAddress() == channel and self.isConnected() : #Address and channel as variables go from 1 to 7
                return True
            else:
                print("The HV board selected doesn't match the channel interested")
                return False
        else:
            return False
    
    def statusString(self, statusCode):
        statuses = {0: 'UP', 1: 'DOWN', 2: 'RUP', 3: 'RDN', 4: 'TUP', 5: 'TDN', 6: 'TRIP'}
        return statuses.get(statusCode, 'undef')
    
    def alarmString(self, alarmCode):
      msg = ' '
      if (alarmCode == 0):
         return 'none'
      if (alarmCode & 1):
         msg = msg + 'OV '
      if (alarmCode & 2):
         msg = msg + 'UV '
      if (alarmCode & 4):
         msg = msg + 'OC '
      if (alarmCode & 8):
         msg = msg + 'OT '
      return msg
    

    def checkConnection(self):
        if(self.isConnected()):
            return True
        else:
            logger_hv.error(f'HV module not connected - use select command')
            return False
        
    def readCalibRegisters(self):
        mlsb = self.dev.read_register(0x0030)
        mmsb = self.dev.read_register(0x0031)
        calibm = ((mmsb << 16) + mlsb)
        calibm = struct.unpack('l', struct.pack('L', calibm & 0xffffffff))[0]
        calibm = calibm / 10000

        qlsb = self.dev.read_register(0x0032)
        qmsb = self.dev.read_register(0x0033)
        calibq = ((qmsb << 16) + qlsb)
        calibq = struct.unpack('l', struct.pack('L', calibq & 0xffffffff))[0]
        calibq = calibq / 10000

        calibt = self.dev.read_register(0x0034)
        calibt = calibt / 1.6890722

        return calibm, calibq, calibt

    def writeCalibSlope(self, slope):
        slope = int(slope * 10000)
        lsb = (slope & 0xFFFF)
        msb = (slope >> 16) & 0xFFFF

        self.dev.write_register(0x0030, lsb)
        self.dev.write_register(0x0031, msb)

    def writeCalibOffset(self, offset):
        offset = int(offset * 10000)
        lsb = (offset & 0xFFFF)
        msb = (offset >> 16) & 0xFFFF

        self.dev.write_register(0x0032, lsb)
        self.dev.write_register(0x0033, msb)

    def writeCalibDiscr(self, discr):
        discr = int(discr * 1.6890722)

        self.dev.write_register(0x0034, discr)
    


    def calibration(self) -> None:
        
        if (self.checkConnection() is False):
            return False


        logger_hv.warning('WARNING: calibration is a time consuming task')
        

        logger_hv.warning('WARNING: erasing current calibration values')
        

        self.writeCalibSlope(1)
        self.writeCalibOffset(0)

        Vexpect = [25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400]
        Vread = []
        
        logger_hv.info('set fast rampup/rampdown rate (25 V/s)')
        self.setRateRampup(25)
        self.setRateRampdown(25)
        
        logger_hv.info('start calibration with status=DOWN Vset=10V')
        self.setVoltageSet(10)
        self.powerOff()
        logger_hv.info(f'waiting for voltage < {Vexpect[0]}')
        while(self.getVoltage() > Vexpect[0]):
            time.sleep(1)
        
        logger_hv.info('turn on high voltage')
        self.powerOn()
        for v in Vexpect:
            logger_hv.info(f"Vset = {v}V")
            self.setVoltageSet(v)
            time.sleep(1)
            logger_hv.info('waiting for voltage level')
            while (True):
                if (self.statusString(self.getStatus()) != 'UP'):
                    time.sleep(1)
                    continue
                else:
                    logger_hv.info(f'Vset = {v}V reached - collecting samples')
                    # wait for voltage leveling
                    time.sleep(2)
                    Vtemp = []
                    for _ in range(0,10):
                        Vtemp.append(self.getVoltage())
                        time.sleep(0.5)
                    Vmeas = np.array(Vtemp)
                    # delete min/max elements
                    Vmeas.sort()
                    Vmeas = np.delete(Vmeas, 0)
                    Vmeas = np.delete(Vmeas, len(Vmeas)-1)
                    Vread.append(Vmeas.mean())
                    logger_hv.info(f'{Vmeas}')
                    logger_hv.info(f'mean = {Vmeas.mean()}')
                    break

        logger_hv.info(f'Vexpect => {Vexpect}')
        logger_hv.info(f'Vread => {Vread}')

        x = np.array(Vread)
        y = np.array(Vexpect)
        # assemble matrix A
        A = np.vstack([x, np.ones(len(x))]).T
        # turn y into a column vector
        y = y[:, np.newaxis]
        # direct least square regression
        alpha = np.dot((np.dot(np.linalg.inv(np.dot(A.T,A)),A.T)),y)
        logger_hv.info(f'slope = {alpha[0][0]} , offset = {alpha[1][0]}')

        # write calibration registers

        self.writeCalibSlope(float(alpha[0][0]))
        self.writeCalibOffset(float(alpha[1][0]))
        logger_hv.info('OK')
            
        logger_hv.info('stop calibration with status=DOWN Vset=10V')
        self.setVoltageSet(10)
        self.powerOff()

        logger_hv.info('calibration DONE!')
        return True
    


    def configure_channel(self, channel, port, voltage_set=None, threshold_set=None, limit_trip_time=None, limit_voltage=None, limit_current=None, limit_temperature=None, rate_up=None, rate_down=None):

        """Function to configure the signle channels with the given parameters"""

        if not self.open(port, channel):
            print(f"It was not possible to open channel: {channel}")
            return False
        
        time.sleep(0.2)
         
        if voltage_set is not None:
            self.setVoltageSet(voltage_set)
            time.sleep(0.2)
        if threshold_set is not None:
            self.setThreshold(threshold_set)
            time.sleep(0.2)
        if limit_trip_time is not None:
            self.setLimitTriptime(limit_trip_time)
            time.sleep(0.2)
        if limit_voltage is not None:
            self.setLimitVoltage(limit_voltage)
            time.sleep(0.2)
        if limit_current is not None:
            self.setLimitCurrent(limit_current)
            time.sleep(0.2)
        if limit_temperature is not None:
            self.setLimitTemperature(limit_temperature)
            time.sleep(0.2)
        if rate_up is not None:
            self.setRateRampup(rate_up)
            time.sleep(0.2)
        if rate_down is not None:
            self.setRateRampdown(rate_down)
            time.sleep(0.2)

        while True:
            if self.statusString(self.getStatus()) == "DOWN":
                break
            else:
                if self.statusString(self.getStatus()) == "UP":
                    break

            time.sleep(2)


        return True
        
    

    def get_channels(self, channels):
        """Function to get which channels """

        if channels == "all":
            channel_list = range(1, 8)
            return channel_list
        else:
            if isinstance(channels, list):
                channel_list = channels
                return channel_list
            else:
                try:
                    channel_list = [int(x) for x in channels.split(",")]
                    return channel_list
                except ValueError:
                    return []




    def process_channels(self, channels, port,**kwargs):

        """Process a list of channels or all of them"""

        valid_channels = []
        not_valid_channels = []

        channel_list = self.get_channels(channels)

        if channel_list == []:
            return [],[]

        
        for channel in channel_list:

            logger_hv.info(f'Configuring channel: {channel}')

            time.sleep(0.1)
            if not self.checkAddressBoundary(channel):
                logger_hv.info(f"Channel {channel} is out of range. Ignored.")
                not_valid_channels.append(channel)
                continue
            
            time.sleep(0.1)
            if not self.check_address(port, channel):
                logger_hv.info("Channel and address selected don't match.")
                not_valid_channels.append(channel)
                continue
            
            time.sleep(0.1)
            if self.configure_channel(channel, port, **kwargs):
                valid_channels.append(channel)
                time.sleep(0.2)
                
            else:
                not_valid_channels.append(channel)
                time.sleep(0.2)

        return valid_channels, not_valid_channels
    

    def set_hv_init_configuration(self, port, channels, voltage_set, threshold_set, limit_trip_time, limit_voltage, limit_current, limit_temperature, rate_up, rate_down):

        """Function to set an initial configuration to the HV board."""

        return self.process_channels(
            channels, port,
            voltage_set=voltage_set,
            threshold_set=threshold_set,
            limit_trip_time=limit_trip_time,
            limit_voltage=limit_voltage,
            limit_current=limit_current,
            limit_temperature=limit_temperature,
            rate_up=rate_up,
            rate_down=rate_down
        )

    def set_voltage(self, channels, voltage_set, port):

        """Function to set only the voltage set to a single or multiple channels"""

        return self.process_channels(channels, port, voltage_set=voltage_set)
    

    
    def set_threshold(self, channels, threshold_set, port):

        """Function to set only the voltage set to a single or multiple channels"""

        return self.process_channels(channels, port, threshold_set=threshold_set)
    


    
    def set_limitI(self, channels, limit_current, port):

        """Function to set only the voltage set to a single or multiple channels"""

        return self.process_channels(channels, port, limit_current=limit_current)
    

    
    def set_limitV(self, channels, limit_voltage, port):

        """Function to set only the voltage set to a single or multiple channels"""

        return self.process_channels(channels, port, limit_voltage=limit_voltage)
    

    
    def set_limitTrip(self, channels, limit_trip_time, port):

        """Function to set only the voltage set to a single or multiple channels"""

        return self.process_channels(channels, port, limit_trip_time=limit_trip_time)
    
    

    def power_on(self, channels, port):


        list_channels = self.get_channels(channels)

        powered_channels = []

        for channel in list_channels:
            logger_hv.info(f"Powering on channel {channel}")
            if self.open(port, channel):
                self.powerOn()
                powered_channels.append(channel)
            else:
                logger_hv.warning(f"Impossible to open/power on channel: {channel}")
                continue



        if not powered_channels:
            logger_hv.warning("No channels were successfully opened.")
            return False

        logger_hv.info(f"Started powering on {len(powered_channels)} channels. Checking status...")


        
        while powered_channels:


            channels_to_remove = []

            for channel in powered_channels:
                if not self.open(port, channel):
                    logger_hv.warning(f"Channel {channel} cannot be opened anymore.")
                    channels_to_remove.append(channel)
                    continue


                alarm = self.alarmString(self.getAlarm())
                if alarm != "none":
                    logger_hv.warning(f"Alarm powering on channel {channel}: {alarm}")
                    channels_to_remove.append(channel)
                    continue


                status = self.statusString(self.getStatus())
                if status == "UP":
                    logger_hv.info(f"Channel {channel} is now UP.")
                    channels_to_remove.append(channel)
                else:
                    pass


            for c in channels_to_remove:
                powered_channels.remove(c)

            if powered_channels:
                time.sleep(2)
                


        if powered_channels:
            logger_hv.warning(f"Some channels never reached UP state: {powered_channels}")
            return False
        else:
            logger_hv.info("All channels are either UP or had an alarm.")
            return True
    

    def channels_calib(self, channels, port):
        list_channels = self.get_channels(channels)
        for channel in list_channels:
            logger_hv.info(f'Calibrating channel {channel}')
            if self.open(port, channel):
                self.calibration()
            else:
                continue


    def power_off(self, channels, port):


        list_channels = self.get_channels(channels)

        powered_channels = []

        for channel in list_channels:
            logger_hv.info(f"Powering off channel {channel}")
            if self.open(port, channel):
                self.powerOff()
                powered_channels.append(channel)
            else:
                logger_hv.warning(f"Impossible to open/power off channel: {channel}")
                continue



        if not powered_channels:
            logger_hv.warning("No channels were successfully opened.")
            return False

        logger_hv.info(f"Started powering off {len(powered_channels)} channels. Checking status...")


        
        while powered_channels:


            channels_to_remove = []

            for channel in powered_channels:
                if not self.open(port, channel):
                    logger_hv.warning(f"Channel {channel} cannot be opened anymore.")
                    channels_to_remove.append(channel)
                    continue


                alarm = self.alarmString(self.getAlarm())
                if alarm != "none":
                    logger_hv.warning(f"Alarm powering off channel {channel}: {alarm}")
                    channels_to_remove.append(channel)
                    continue


                status = self.statusString(self.getStatus())
                if status == "DOWN":
                    logger_hv.info(f"Channel {channel} is now DOWN.")
                    channels_to_remove.append(channel)
                else:
                    pass


            for c in channels_to_remove:
                powered_channels.remove(c)

            if powered_channels:
                time.sleep(2)
                


        if powered_channels:
            logger_hv.warning(f"Some channels never reached DOWN state: {powered_channels}")
            return False
        else:
            logger_hv.info("All channels are either DOWN or had an alarm.")
            return True
    

    def read_volt(self, channels, port):
        """Function to monitor the voltages of the FEBs for different channels."""

        if isinstance(channels, list):
            hv_list = channels
        else:
            try:
                hv_list = [int(x) for x in channels.split(",")]
            except ValueError:
                print('E: failed to parse channels - should be a comma-separated list of integers')
                return None

        hv_value = {}
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')

        hv_value["type"] = "data"
        hv_value["data_type"] = "hv_data"
        baseAddress = 0x0000
        regs = self.dev.read_registers(baseAddress, 48)
        for hv in hv_list:
            if self.open(port, hv):
                hv_value[hv] = {
                    'time': timestamp,
                    'V': self.getVoltage(),
                    'I': self.getCurrent(),
                    'T' : self.convert_temp(self.getTemperature())
                }
            time.sleep(1)

                

        return hv_value
    




    




            
        