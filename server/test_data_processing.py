import logging
import subprocess
import datetime
import os
import time
from pathlib import Path

#########################################
logger = logging.getLogger("DataProcessing")
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Error File Handler
processor_error_handler = logging.FileHandler('processor_error.log')
processor_error_handler.setLevel(logging.WARNING)
processor_error_handler.setFormatter(formatter)
logger.addHandler(processor_error_handler)
#########################################



#Dictionary to set the folder based on the acquisition type => "acquisition_type" : "folder_name"

folder_acq = {


    "polarizer" : "polarizer_calibration/",
    "pedestal" : "pedestal_characterisation/",
    "spe" : "single_photoelectron/",
    "gain" : "gain_curve/",
    "wheels_char" : "wheels_characterisation/",
    "fiber_char" : "fiber_characterisation/",
    "threshold": "threshold_calibration/",
    "threshold_dark": "threshold_calibration_dark/",
    "threshold_scan": "threshold_scan/",
    "spe_equal": "spe_equal_gains/"
}

class DataProcess:

    def __init__(self):
        self.server = None
        self.opened_files = []
        logger.debug("DataProcess initialized with port %s", self.port)

    @staticmethod
    def generate_timestamp():
        return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')
    
    @staticmethod
    def generate_timestamp_folder():
        return datetime.datetime.now().strftime('%Y_%m_%d')
    
    @staticmethod
    def get_file_name(suffix):
        timestamp = DataProcess.generate_timestamp()
        file_prefix = "daq"
        return f"{file_prefix}_{timestamp}_{suffix}.csv"
    
    @staticmethod
    def check_file_exists(fname):
        base, ext = os.path.splitext(fname)
        i = 1
        while os.path.exists(fname):
            fname = f"{base}_{i}{ext}"
            i += 1
        return fname



    def string_no_space(self, string):
        return string.replace(" ", "")
    
    def get_folder_path(self, flag_acq = "", run_id = None, number = None):


        base_path = Path("/swgo") if Path("/swgo").exists() else Path.home()

        base_folder = base_path / "multiPMT" / "calibration" / f"batch_{number}" / folder_acq.get(flag_acq, "unknown") / DataProcess.generate_timestamp_folder()

        if run_id is not None:
            run_folder = base_folder / f"run_{run_id}"
        else:
            i = 1
            run_folder = base_folder / f"acq_{i}"
            while run_folder.exists():
                i += 1
                run_folder = base_folder / f"acq_{i}"

        run_folder.mkdir(parents=True, exist_ok=True)
        return run_folder


    def run(self, duration=None, suffix="", flag_acq = "", run_id = None, number = None): 

        
        
        run_folder = self.get_folder_path(flag_acq=flag_acq, run_id=run_id, number=number)

        filename = self.check_file_exists(DataProcess.get_file_name(suffix))
        filepath = run_folder / filename
        filepath = Path(filepath).expanduser()

        process = subprocess.Popen(["./evreceiver", str(filepath)])
        
        
        try:
            if duration:
                time.sleep(duration)
                process.terminate()
                process.wait(timeout=5)
            else:
                process.wait()
        except Exception as e:
            logger.error(f"Error managing parser process: {e}")
            process.kill()



    