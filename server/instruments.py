from pylablib.devices import Thorlabs
import logging



logger = logging.getLogger("Wheels")


class Wheels:
    def __init__(self, port):

        self.port = port

        try:
            self.wheel = Thorlabs.FW(self.port)
            logger.info(f"Wheel initialized on port {port}")
        except Exception as e:
            logger.error(f"Failed to initialize wheel on port {port}: {e}")
            raise

    def device_info(self):
        try:
            id = self.wheel.get_id()
            positions = self.wheel.get_pcount()
            current_pos= self.wheel.get_position()
            self.wheel.wait_sync()
            return(id, positions, current_pos)
        except Exception as e:
            logger.error(f"Unable to retrieve info from device on port {self.port}: {e}")
            raise

    def go_to_position(self, position):
        try:
            current_pos = self.wheel.get_position()

            if position != current_pos:
                self.wheel.set_position(position)
                self.wheel.wait_sync()
                logger.info(f"Wheel on port {self.port} moved to position {position}")

        except Exception as e:
            logger.error(f"Unable to change position of the device on port {self.port}: {e}")
            raise
    
    def close(self):
        """Release the wheel resources"""
        try:
            self.wheel.close()
            logger.info(f"Wheel on port {self.port} successfully closed.")
        except Exception as e:
            logger.warning(f"Failed to close wheel on port {self.port}: {e}")



class Polarizer:
    def __init__(self, port):
        self.port=port
        conn = {'port': str(self.port), 'baudrate':'115200', 'rtscts':'True'}
        self.stepper= Thorlabs.KinesisMotor(conn, scale= 'stage')     
    
    def device_info(self):
        try:
            info_stage = self.stepper.get_stage()
            info_pos = self.stepper.get_position()
            info_home = self.stepper.get_homing_parameters(scale=True)
            return(info_stage,info_pos,info_home)
        
        except Exception as e:
            logger.error(f"Unable to retrieve info from device on port {self.port}: {e}")
            raise

    def go_to_position(self, position):
        position = float(position)
        current_position = float(self.stepper.get_position(scale=True))
        print(position, current_position)
        try:
            if position != current_position:  
                self.stepper.move_to(position, scale=True)
                self.stepper.wait_move()
                new_pos= self.stepper.get_position(scale=True)
                logger.info(f"Polarizer in {new_pos} position")

        except Exception as e:
            logger.error(f"Unable to change position of the device on port {self.port}: {e}")
            raise
    
    def close(self):
        """Release the polarizer resources"""
        try:
            self.stepper.close()
            logger.info(f"Polarizer on port {self.port} successfully closed.")
        except Exception as e:
            logger.warning(f"Failed to close polarizer on port {self.port}: {e}")










