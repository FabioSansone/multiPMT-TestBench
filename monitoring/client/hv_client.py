import pymodbus.client as ModbusClient
from pymodbus import (
    FramerType,
    ModbusException
)
import logging
import struct
import time
import numpy as np
import datetime
import traceback

hv_logger = logging.getLogger("pymodbus")
hv_logger.warning("Avviso da pymodbus dentro hv.py")



#NB#
# addr corrisponde al channel della scheda mentre port corrisponde alla seriale del tipo /dev/ttyPS1
# La porta indicata nella funzione ModbusTcpClient è quella per la connessione tcp
# La porta seriale per la connessione della scheda è inclusa dentro params
###

class HVReadError(Exception):
    pass


class HV:

    def __init__(self, params) -> None:

        self.client = None
        self.params = params
        self.dev = None
        self.addr = None


        if self.params.mode == 'tcp':
            self.client = ModbusClient.ModbusTcpClient(self.params.host, port=502, framer=FramerType.SOCKET) 
            if not self.client.connect():
                raise ConnectionError(f"Host not reachable or mbusd not running ({self.params.host})")

        elif self.params.mode == 'rtu':
         
            self.client = ModbusClient.ModbusSerialClient(
                self.params.port, 
                framer=FramerType.RTU, 
                baudrate=115200,
                bytesize=8,
                parity="N",
                stopbits=1,
                timeout=1
        )

            if not self.client.connect():
                raise ConnectionError(f"Host not reachable or mbusd not running ({self.params.host})")
 
    
    def _safe_read(self, addr, count, slave, desc="unknown"):
        rr = None
        try:
            rr = self.client.read_holding_registers(address=addr, count=count, slave=slave)
            if rr is None or rr.isError():
                raise HVReadError(f"Invalid response when reading {desc} at {hex(addr)}")
            return rr.registers
        except ModbusException as e:
            raise HVReadError(f"Exception during read of {desc}: {e}")
    
    def _safe_write(self, addr, value, slave, desc="unknown"):
        try:
            rr = self.client.write_register(address=addr, value=value, slave=slave)
            if rr is None or rr.isError():
                raise HVReadError(f"Invalid response when writing {desc} at {hex(addr)}")
        except ModbusException as e:
            raise HVReadError(f"Exception during write of {desc}: {e}")



    
    def handleInterrupt(self):
        try:
            if hasattr(self.client, "connected") and not self.client.connected:
                self.client.close()
        except Exception as e:
            hv_logger.warning(f"Errore durante il controllo dello stato del client: {e}")




    def open(self, addr):

        self.handleInterrupt()

        rr = None
        try:
            rr = self.client.read_holding_registers(address=0, count=1, slave=addr)
        except ModbusException as e:
            hv_logger.error(e)
            return False 

        if rr.isError() or rr is None:
            hv_logger.error("Problem occured opening the selected modbus address")
            return False
        
        self.addr = addr
        return True
    
    def checkAddressBoundary(self, addr):
        return addr >= 1 and addr <= 20
    
    def isConnected(self):
        return self.addr is not None
    
    def getAddress(self):
        return self.addr
    
    def checkConnection(self):
        if not self.isConnected():
            hv_logger.error("Was not possible to check for connection")
            return False
        return True

        
    def checkAddress(self, addr):
        if self.open(addr):
            if self.getAddress() == addr and self.isConnected() : #Address and channel as variables go from 1 to 7
                return True
            else:
                hv_logger.warning("The HV board selected doesn't match the channel interested")
                return False
        else:
            hv_logger.warning("It was not possible to check the address: error in opening the selected modbus address")
            return False
    
    def setModbusAddress(self, addr, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x00, value=addr, slave=slave, desc="address set")


    def getStatus(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(address=6, count=1, slave=slave, desc="status")
        return rr[0]

            

    def getVoltage(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x2A, count=2, slave=slave, desc="voltage")
        rr.reverse()
        return self.client.convert_from_registers(rr, data_type=self.client.DATATYPE.INT32) / 1000

        
    def getVoltageSet(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x26, count=1, slave=slave, desc="voltage set")
        return rr[0]


    def setVoltageSet(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x26, value=value, slave=slave, desc="voltage set")

    
    def getCurrent(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x28, count=2, slave=slave, desc="current")
        rr.reverse()
        return self.client.convert_from_registers(rr, data_type=self.client.DATATYPE.INT32) / 1000

    
    def getTemperature(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x07, count=1, slave=slave, desc="temperature")
        return rr[0]

    
    def convertTemperature(self, value):
        q = (value & 0xFF) / 1000
        i = (value >> 8) & 0xFF
        return round(q+i, 2)
    
    def getRate(self, fmt=str, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x23, count=2, slave=slave, desc="rate")
        rup = rr[0]
        rdn = rr[1]
        return f'{rup}/{rdn}' if fmt == str else (rup, rdn)

    
    def setRateRampup(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x23, value=value, slave=slave, desc="rate ramp-up")

    
    def setRateRampdown(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x24, value=value, slave=slave, desc="rate ramp-down")

    
    def getLimit(self, fmt=str, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0, count=48, slave=slave, desc="limits")
        lv = rr[0x27]
        li = rr[0x25]
        lt = rr[0x2F]
        ltt = rr[0x22]
        return f'{lv}/{li}/{lt}/{ltt}' if fmt == str else (lv, li, lt, ltt)


    def setLimitVoltage(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x27, value=value, slave=slave, desc="limit voltage")

    
    def setLimitCurrent(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x25, value=value, slave=slave, desc="limit current")

    
    def setLimitTemperature(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x2F, value=value, slave=slave, desc="limit temperature")

    
    def setLimitTriptime(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x22, value=value, slave=slave, desc="limit trip time")

    
    def setThreshold(self, value, slave=None):
        slave = self.addr if slave is None else slave
        self._safe_write(addr=0x2D, value=value, slave=slave, desc="threshold")

    
    def getThreshold(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x2D, count=1, slave=slave, desc="threshold")
        return rr[0]

    
    def getAlarm(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x2E, count=1, slave=slave, desc="alarm")
        return rr[0]

    
    def getVref(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x2E, count=1, slave=slave, desc="vref")
        return rr[0] / 10

    
    def powerOn(self, slave=None):
        slave = self.addr if slave==None else slave
        try:
            self.client.write_coil(address=1, value=True, slave=slave)
        except ModbusException as e:
            hv_logger.error(e)
            raise e
    
    def powerOff(self, slave=None):
        slave = self.addr if slave==None else slave
        try:
            self.client.write_coil(address=1, value=False, slave=slave)
        except ModbusException as e:
            hv_logger.error(e)
            raise e
    
    def reset(self, slave=None):
        slave = self.addr if slave==None else slave
        try:
            self.client.write_coil(address=2, value=True, slave=slave)
        except ModbusException as e:
            hv_logger.error(e)
            raise e
    
    def getInfo(self, slave=None):
        slave = self.addr if slave==None else slave
        l = None
        try:
            l = self.client.read_holding_registers(address=0x02, count=1, slave=slave).registers
            fwver = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x08, count=6, slave=slave).registers
            pmtsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x0E, count=6, slave=slave).registers
            hvsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x14, count=6, slave=slave).registers
            febsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x04, count=2, slave=slave).registers
            devid = (l[1] << 16) + l[0]
            return (fwver, pmtsn, hvsn, febsn, devid)
        except ModbusException as e:
            hv_logger.error(e)
            raise e
    
    def readMonRegisters(self, slave=None):
        slave = self.addr if slave==None else slave
        rr = None
        monData = {}
        try:
            rr = self.client.read_holding_registers(address=0, count=48, slave=slave)
            monData['status'] = rr.registers[0x0006]
            monData['Vset'] = rr.registers[0x0026]
            monData['V'] = ((rr.registers[0x002B] << 16) + rr.registers[0x002A]) / 1000
            monData['I'] = ((rr.registers[0x0029] << 16) + rr.registers[0x0028]) / 1000
            monData['T'] = self.convertTemperature(rr.registers[0x0007])
            monData['rateUP'] = rr.registers[0x0023]
            monData['rateDN'] = rr.registers[0x0024]
            monData['limitV'] = rr.registers[0x0027]
            monData['limitI'] = rr.registers[0x0025]
            monData['limitT'] = rr.registers[0x002F]
            monData['limitTRIP'] = rr.registers[0x0022]
            monData['threshold'] = rr.registers[0x002D]
            monData['alarm'] = rr.registers[0x002E]
            return monData
        except ModbusException as e:
            hv_logger.error(e)
            raise e


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

    def readCalibRegisters(self, slave=None):
        slave = self.addr if slave is None else slave
        rr = self._safe_read(addr=0x30, count=5, slave=slave, desc="calib reg")
        mlsb = rr.registers[0]
        mmsb = rr.registers[1]
        qlsb = rr.registers[2]
        qmsb = rr.registers[3]
        calibt = rr.registers[4]

        calibm = ((mmsb << 16) + mlsb)
        calibm = struct.unpack('l', struct.pack('L', calibm & 0xffffffff))[0]
        calibm = calibm / 10000

        calibq = ((qmsb << 16) + qlsb)
        calibq = struct.unpack('l', struct.pack('L', calibq & 0xffffffff))[0]
        calibq = calibq / 10000

        calibt = calibt / 1.6890722

        return (calibm, calibq, calibt)


    def writeCalibSlope(self, slope, slave=None):
        slave = self.addr if slave is None else slave
        slope = int(slope * 10000)
        lsb = (slope & 0xFFFF)
        msb = (slope >> 16) & 0xFFFF
        self._safe_write(addr=0x30, value=[lsb, msb], slave=slave, desc="write slop")

    def writeCalibOffset(self, offset, slave=None):
        slave = self.addr if slave is None else slave
        offset = int(offset * 10000)
        lsb = (offset & 0xFFFF)
        msb = (offset >> 16) & 0xFFFF
        self._safe_write(addr=0x32, value=[lsb, msb], slave=slave, desc="write offset")
    
    def writeCalibDiscr(self, discr, slave=None):
        slave = self.addr if slave is None else slave
        discr = int(discr * 1.6890722)
        self._safe_write(addr=0x34, value=discr, slave=slave, desc="write discr")
    

    def calibration(self) -> None:
        
        if (self.checkConnection() is False):
            return False


        hv_logger.warning('WARNING: calibration is a time consuming task')
        

        hv_logger.warning('WARNING: erasing current calibration values')
        
        try:
            self.writeCalibSlope(1)
            self.writeCalibOffset(0)
        except Exception as e:
            hv_logger.error(f"Error in initial calibration write: {e}")
            hv_logger.error(traceback.format_exc())
            return False


        Vexpect = [25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400]
        Vread = []
        
        hv_logger.warning('set fast rampup/rampdown rate (25 V/s)')
        try:
            self.setRateRampup(25)
            self.setRateRampdown(25)
        except Exception as e:
            hv_logger.error(f"Error setting ramp rates: {e}")
            return False
        
        hv_logger.warning('start calibration with status=DOWN Vset=10V')
        self.setVoltageSet(10)
        self.powerOff()
        hv_logger.warning(f'waiting for voltage < {Vexpect[0]}')
        while(self.getVoltage() > Vexpect[0]):
            time.sleep(1)
        
        hv_logger.warning('turn on high voltage')
        self.powerOn()
        for v in Vexpect:
            hv_logger.info(f"Vset = {v}V")
            self.setVoltageSet(v)
            time.sleep(1)
            hv_logger.info('waiting for voltage level')
            while (True):
                if (self.statusString(self.getStatus()) != 'UP'):
                    time.sleep(1)
                    continue
                else:
                    hv_logger.info(f'Vset = {v}V reached - collecting samples')
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
                    hv_logger.warning(f'{Vmeas}')
                    hv_logger.warning(f'mean = {Vmeas.mean()}')
                    break

        hv_logger.warning(f'Vexpect => {Vexpect}')
        hv_logger.warning(f'Vread => {Vread}')

        x = np.array(Vread)
        y = np.array(Vexpect)
        # assemble matrix A
        A = np.vstack([x, np.ones(len(x))]).T
        # turn y into a column vector
        y = y[:, np.newaxis]
        # direct least square regression
        alpha = np.dot((np.dot(np.linalg.inv(np.dot(A.T,A)),A.T)),y)
        hv_logger.warning(f'slope = {alpha[0][0]} , offset = {alpha[1][0]}')

        # write calibration registers

        self.writeCalibSlope(float(alpha[0][0]))
        self.writeCalibOffset(float(alpha[1][0]))
        hv_logger.warning('OK')
            
        hv_logger.warning('stop calibration with status=DOWN Vset=10V')
        self.setVoltageSet(10)
        self.powerOff()

        hv_logger.warning('calibration DONE!')
        return True
    
    def channelsCalib(self, channels):
        list_channels = self.getChannels(channels)
        for channel in list_channels:
            hv_logger.warning(f'Calibrating channel {channel}')
            if self.open(channel):
                self.calibration()
            else:
                continue
        
        return True
    

    def getChannels(self, channels):
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
    

    def configureChannel(self, channel,
                        voltage_set=None, threshold_set=None, limit_trip_time=None, limit_voltage=None, limit_current=None, limit_temperature=None, 
                        rate_up=None, rate_down=None):

        if not self.open(channel):
            hv_logger.warning(f"It was not possible to open channel: {channel}")
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
            elif self.statusString(self.getStatus()) == "UP":
                break

            time.sleep(2)


        return True


    def processChannels(self, channels, **kwargs):
        valid_channels = []
        not_valid_channels = []

        channel_list = self.getChannels(channels)

        if channel_list == []:
            return [],[]

        
        for channel in channel_list:

            hv_logger.warning(f'Configuring channel: {channel}')

            time.sleep(0.1)
            if not self.checkAddressBoundary(channel):
                hv_logger.warning(f"Channel {channel} is out of range. Ignored.")
                not_valid_channels.append(channel)
                continue
            
            time.sleep(0.1)
            if not self.checkAddress(channel):
                hv_logger.warning("Channel and address selected don't match.")
                not_valid_channels.append(channel)
                continue
            
            time.sleep(0.1)
            if self.configureChannel(channel, **kwargs):
                valid_channels.append(channel)
                time.sleep(0.2)
                
            else:
                not_valid_channels.append(channel)
                time.sleep(0.2)

        return valid_channels, not_valid_channels

    def setInitConfiguration(self, channels, 
                            voltage_set, threshold_set, limit_trip_time, limit_voltage, limit_current, limit_temperature,
                            rate_up, rate_down):


        return self.processChannels(
            channels,
            voltage_set=voltage_set,
            threshold_set=threshold_set,
            limit_trip_time=limit_trip_time,
            limit_voltage=limit_voltage,
            limit_current=limit_current,
            limit_temperature=limit_temperature,
            rate_up=rate_up,
            rate_down=rate_down
        )
    
    

    def set_voltage(self, channels, voltage_set):
        return self.processChannels(channels, voltage_set=voltage_set)
    

    
    def set_threshold(self, channels, threshold_set):
        return self.processChannels(channels, threshold_set=threshold_set)
    


    
    def set_limitI(self, channels, limit_current):
        return self.processChannels(channels, limit_current=limit_current)
    

    
    def set_limitV(self, channels, limit_voltage):
        return self.processChannels(channels, limit_voltage=limit_voltage)
    

    
    def set_limitTrip(self, channels, limit_trip_time):
        return self.processChannels(channels, limit_trip_time=limit_trip_time)
    

    def power_on(self, channels):
        list_channels = self.getChannels(channels)
        powered_channels = []

        for channel in list_channels:
            hv_logger.warning(f"Powering on channel {channel}")
            if self.open(channel):
                self.powerOn()
                powered_channels.append(channel)
            else:
                hv_logger.error(f"Impossible to open/power on channel: {channel}")
                continue



        if not powered_channels:
            hv_logger.error("No channels were successfully opened.")
            return False

        hv_logger.warning(f"Started powering on {len(powered_channels)} channels. Checking status...")


        
        while powered_channels:
            channels_to_remove = []

            for channel in powered_channels:
                if not self.open(channel):
                    hv_logger.error(f"Channel {channel} cannot be opened anymore.")
                    channels_to_remove.append(channel)
                    continue


                alarm = self.alarmString(self.getAlarm())
                if alarm != "none":
                    hv_logger.warning(f"Alarm powering on channel {channel}: {alarm}")
                    channels_to_remove.append(channel)
                    continue


                status = self.statusString(self.getStatus())
                if status == "UP":
                    hv_logger.info(f"Channel {channel} is now UP.")
                    channels_to_remove.append(channel)
                else:
                    pass


            for c in channels_to_remove:
                powered_channels.remove(c)

            if powered_channels:
                time.sleep(2)
            

        if powered_channels:
            hv_logger.error(f"Some channels never reached UP state: {powered_channels}")
            return False
        else:
            hv_logger.warning("All channels are either UP or had an alarm.")
            return True
        
    
    def power_off(self, channels):
        list_channels = self.getChannels(channels)
        powered_channels = []

        for channel in list_channels:
            hv_logger.warning(f"Powering off channel {channel}")
            if self.open(channel):
                self.powerOff()
                powered_channels.append(channel)
            else:
                hv_logger.error(f"Impossible to open/power off channel: {channel}")
                continue



        if not powered_channels:
            hv_logger.error("No channels were successfully opened.")
            return False

        hv_logger.warning(f"Started powering off {len(powered_channels)} channels. Checking status...")


        
        while powered_channels:
            channels_to_remove = []

            for channel in powered_channels:
                if not self.open(channel):
                    hv_logger.warning(f"Channel {channel} cannot be opened anymore.")
                    channels_to_remove.append(channel)
                    continue


                alarm = self.alarmString(self.getAlarm())
                if alarm != "none":
                    hv_logger.warning(f"Alarm powering off channel {channel}: {alarm}")
                    channels_to_remove.append(channel)
                    continue


                status = self.statusString(self.getStatus())
                if status == "DOWN":
                    hv_logger.warning(f"Channel {channel} is now DOWN.")
                    channels_to_remove.append(channel)
                else:
                    pass


            for c in channels_to_remove:
                powered_channels.remove(c)

            if powered_channels:
                time.sleep(2)
                


        if powered_channels:
            hv_logger.error(f"Some channels never reached DOWN state: {powered_channels}")
            return False
        else:
            hv_logger.warning("All channels are either DOWN or had an alarm.")
            return True
        
    

    def readVolt(self, channels, offset = None):
        hv_list = self.getChannels(channels)

        hv_value = {}
        if offset is None:
            timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')
        else:
            timestamp = (datetime.datetime.now() + offset).strftime('%Y_%m_%d_%H_%M')

        hv_value["type"] = "data"
        hv_value["data_type"] = "hv_data"
        for hv in hv_list:
            if self.open(hv):
                hv_value[hv] = {
                    'time': timestamp,
                    'V': self.getVoltage(),
                    'I': self.getCurrent(),
                    'T' : self.convertTemperature(self.getTemperature())
                }
            time.sleep(1)

                

        return hv_value

    def setPMTSerialNumber(self, sn, slave=None):
        slave = self.address if slave == None else slave
        data = list(bytes(sn.ljust(12), 'utf-8'))
        self._safe_write(address=0x08, values=data, slave=slave, desc = "pmt serial write")
    
    def getSerial(self, channels):
        info = {}
        hv_list = self.getChannels(channels)
        hv_logger.warning(f"Canali ottenuti: {list(hv_list)}")
        for hv in hv_list:
            if self.open(hv):
                try:
                    a = self.getInfo(hv)
                    info[hv] = [a[-1], a[1], a[3], a[2]]
                    hv_logger.info(f"Channel {hv}: Device ID {a[-1]}, PMT Serial {a[1]}, FEB_serial {a[3]}, HV Serial {a[2]}")
                except Exception as e:
                    hv_logger.error(f"Error reading serial for channel {hv}: {e}")
                time.sleep(1)
            else:
                hv_logger.warning(f"Channel {hv} non aperto correttamente.")
        return info

    
    
    
        




    def close(self):
        if self.client:
            self.client.close()
