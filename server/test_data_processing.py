import logging
import subprocess
import datetime
import os
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
        self.opened_files = []

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
    
    def get_folder_path(self, flag_acq = "", run_id = None, number = None):
        base_path = Path("/swgo") if Path("/swgo").exists() else Path.home()

        base_folder = base_path / "multiPMT" / "acquisition" / f"batch_{number}" / folder_acq.get(flag_acq, "unknown") / DataProcess.generate_timestamp_folder()

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

    

    def compile_evreceiver(self, force_compile=False):
        build_dir = Path(__file__).parent / "../evreceiver"
        evr_exe = build_dir / "evreceiver"
        if not evr_exe.exists() or force_compile:
            logger.warning("Compiling evreceiver executable...")
            compile_cmd = ["gcc", str(build_dir / "evreceiver.c"), "-o", str(evr_exe), "-lzmq", "-lpthread", "-O2"]
            result = subprocess.run(compile_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Compilation failed:\n{result.stderr}")
            logger.warning("Compilation completed successfully.")
        return evr_exe
            
    def run(self, duration=None, suffix="", flag_acq = "", run_id = None, number = None, force_compile=False): 


        #flush_process = subprocess.run([str(flush_exe), "30"], capture_output=True, text=True)
        #if flush_process.returncode != 0:
        #    logger.error(f"flush_fifo failed:\n{flush_process.stderr}")
        logger.info("Starting acquisition... ")
        run_folder = self.get_folder_path(flag_acq=flag_acq, run_id=run_id, number=number)
        filename = self.check_file_exists(DataProcess.get_file_name(suffix))
        filepath = run_folder / filename

        evr_exe = self.compile_evreceiver(force_compile=force_compile)
        logger.info("Running evreceiver")

        
        main_process = subprocess.Popen([str(evr_exe), str(filepath), str(duration or -1)])
        return main_process

        

if __name__ == "__main__":

    evrecv = DataProcess()

    evrecv.run(60, "test", "test", "test", force_compile=True)


    