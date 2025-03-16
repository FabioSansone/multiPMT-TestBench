from instruments import Wheels, Polarizer
from typing import Callable
import logging

logger = logging.getLogger("Server")


class InstrumentsManager:
    def __init__(self, output_func: Callable[[str], None]):
        """
        Class dedicated to the management of optical instruments: Polarizer and Wheels
        """
        self.near_wheel = None
        self.far_wheel = None
        self.polarizer = None
        self.output = output_func

    def init_wheels(self, near_wheel_pos:int, far_wheel_pos:int) -> None:
        """
        Wheels initialisation and position settings
        """
        try:
            # Initialize near wheel
            self.near_wheel = Wheels('/dev/ttyUSB1')
            near_id, positions, current_pos = self.near_wheel.device_info()
            self.output(f"Near wheel ID: {near_id}, Positions: {positions}, Current: {current_pos}")
            
            # Initialize far wheel
            self.far_wheel = Wheels('/dev/ttyUSB2')
            far_id, positions, current_pos = self.far_wheel.device_info()
            self.output(f"Far wheel ID: {far_id}, Positions: {positions}, Current: {current_pos}")
            
            # Set positions
            self.near_wheel.go_to_position(near_wheel_pos)
            self.output(f"Near wheel moved to position {near_wheel_pos}")
            
            self.far_wheel.go_to_position(far_wheel_pos)
            self.output(f"Far wheel moved to position {far_wheel_pos}")

        except Exception as e:
            logger.error(f"Error during wheel initialization: {e}")
            self.output(f"Error: {e}")
            
            if self.near_wheel:
                self.near_wheel.close()
            if self.far_wheel:
                self.far_wheel.close()

    def init_polarizer(self, pol_position:int) -> None:
        """
        Polarizer initialisation and position settings
        """
        try:
            #Initialise Polarizer
            self.polarizer = Polarizer('/dev/ttyUSB0')
            pol_name, pol_pos, pol_para = self.polarizer.device_info()
            self.output(f"Polarizer name: {pol_name}, Position: {pol_pos}, Parameters: {pol_para}")
            
            #Set Polarizer Position
            self.polarizer.go_to_position(pol_position)
            self.output(f"Polarizer moved to position {pol_position}")
        except Exception as e:
            logger.error(f"Error during polarizer initialization: {e}")
            self.output(f"Error: {e}")
            if self.polarizer:
                self.polarizer.close()