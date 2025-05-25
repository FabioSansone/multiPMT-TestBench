import logging
import csv
from typing import Dict
from pathlib import Path
import os
import datetime
from contextlib import contextmanager

logger = logging.getLogger("Server")

folder_clients = {
    "rc_data" : "rate",
    "hv_data" : "hv",
    "mon_data" : "mon"}

folder_acq = {

    "threshold_scan": "threshold_scan"

}

opened_files = {}

def GenerateTimestamp()->datetime:
    return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')


def GetFileName(data_flag:str, suffix: str)->str:
    timestamp = GenerateTimestamp()
    file_map = {
        "rc_data": "rcmon",
        "hv_data": "hvmon",
        "mon_data": "mon"
    }
    file_prefix = file_map.get(data_flag, "unknown")
    if suffix == "":
        return f"{file_prefix}_{timestamp}.csv"
    else:
        return f"{file_prefix}_{timestamp}_{suffix}.csv"

def CheckFileExists(fname:str)->str:
    base, ext = os.path.splitext(fname)
    i = 1
    while os.path.exists(fname):
        fname = f"{base}_{i}{ext}"
        i+=1
    return fname

@contextmanager
def OpenFiles(client: bytes, data_flag: str, number: int, flag_acq: str, suffix: str, run_id: str ="monitoring"):
    
    key = f"{client.decode()}_{data_flag}_{suffix}"
    if key not in opened_files:
        filename=CheckFileExists(GetFileName(data_flag=data_flag, suffix=suffix))
        if flag_acq == "":
            filepath= Path(f"/swgo/Test/SWGO_Testbench/multiPMT/calibration/batch_{number}") / folder_clients.get(data_flag, 'unknown') /  run_id / filename
        else:
            filepath= Path(f"/swgo/Test/SWGO_Testbench/multiPMT/calibration/batch_{number}") / folder_acq.get(flag_acq, 'unknown') /  run_id / filename

        filepath.parent.mkdir(parents=True, exist_ok=True)
        file = open(filepath, 'a', newline='')
        writer = csv.writer(file)

        if data_flag == "rc_data":
            writer.writerow(['register', 'time', 'int_value'])

        elif data_flag == "hv_data":
            writer.writerow(['address', 'time', 'V', 'I', 'T'])

        elif data_flag == "mon_data":
            writer.writerow(['time', 'temp', 'pressure', 'hum', '5V', '3V3', 'I'])
     
        file.flush()

        opened_files[key] = file

    else:
        file = opened_files[key]

    try:
        yield file
    finally:
        file.flush()


def WriteCSV(client: bytes, data_flag: str, data: Dict, number: int, flag_acq: str, suffix: str, run_id: str)->None:

        excluded_key = {"type", "data_type"}
        if data_flag == "rc_data":
            with OpenFiles(client=client, data_flag=data_flag, number=number, flag_acq=flag_acq, suffix=suffix, run_id=run_id) as file_rc:
                writer = csv.writer(file_rc)
                for reg_address, reg_info in data.items():
                    if reg_address not in excluded_key:
                        writer.writerow([reg_address, reg_info["time"], reg_info["value"]])

                
        if data_flag == "hv_data":
            with OpenFiles(client=client, data_flag=data_flag, number=number, flag_acq=flag_acq, suffix=suffix, run_id=run_id) as file_hv:
                writer = csv.writer(file_hv)
                for channel, channel_info in data.items():
                    if channel not in excluded_key:
                        writer.writerow([channel, channel_info["time"], channel_info["V"], channel_info["I"], channel_info["T"]])


        if data_flag == "mon_data":
            with OpenFiles(client=client, data_flag=data_flag, number=number, flag_acq=flag_acq, suffix=suffix, run_id=run_id) as file_mon:
                writer = csv.writer(file_mon)
                for board, board_info in data.items():
                    if board not in excluded_key:
                        writer.writerow([board_info["time"], board_info["temp"], board_info["pressure"], board_info["hum"], board_info["5V"], board_info["3V3"], board_info["I"]])



def SaveDataCSV(client: bytes, data: Dict, number: int, flag_acq: str, suffix: str, run_id: str)-> None:
    try:
        if data.get("type") == "data":
            data_type = data.get("data_type")
            WriteCSV(client=client, data_flag=data_type, data=data, number=number, flag_acq=flag_acq, suffix=suffix, run_id=run_id)  

        else:
            logger.warning("Received message is not of type 'data', ignoring.")

    except Exception as e:
        logger.error(f"Error while saving data to CSV: {e}")


def CloseOpenFile(client: bytes, data_flag: str, suffix: str):
    key = f"{client.decode()}_{data_flag}_{suffix}"
    file = opened_files.get(key)
    if file:
        try:
            file.flush()
            file.close()
        except Exception as e:
            logger.warning(f"Failed to close file {key}: {e}")
        del opened_files[key]


def WriteSerialChannels(serial_data: dict, number: int, extra_columns: dict = None) -> None:
    """
    Scrive i dati dei canali seriali in un file CSV nella cartella batch_{number}.
    Se il file esiste, aggiorna solo le righe mancanti o aggiunge nuove colonne senza duplicati.
    """
    if extra_columns is None:
        extra_columns = {}

    
    base_path = Path(f"/swgo/Test/SWGO_Testbench/multiPMT/calibration/batch_{number}")
    base_path.mkdir(parents=True, exist_ok=True)
    filepath = base_path / "MultiPMT_Info.csv"

    
    existing_rows = {}
    existing_headers = ['channel', 'device_id', 'pmt_serial', 'feb_serial', 'hv_serial']
    if filepath.exists():
        with open(filepath, 'r', newline='') as file:
            reader = csv.DictReader(file)
            existing_headers = reader.fieldnames
            for row in reader:
                channel = int(row['channel'])
                existing_rows[channel] = row

    #Sto pensando ad un dizionario con i canali come chiavi e gli item come dizionari che contengno tensione e soglia di ogni canale
    extra_keys = set()
    for extras in extra_columns.values():
        extra_keys.update(extras.keys())
    all_headers = existing_headers or []
    for k in extra_keys:
        if k not in all_headers:
            all_headers.append(k)

    
    for channel, serial_info in serial_data.items():
        row = {
            'channel': channel - 1,
            'device_id': serial_info[0],
            'pmt_serial': serial_info[1],
            'feb_serial': serial_info[2],
            'hv_serial': serial_info[3]
        }

        
        extras = extra_columns.get(channel, {})
        row.update(extras)

        # Se esiste già, confronta. Altrimenti aggiungi.
        existing_row = existing_rows.get(channel - 1)
        if existing_row:
            if all(str(existing_row.get(k, "")) == str(row.get(k, "")) for k in all_headers):
                continue  
        existing_rows[channel - 1] = row

    
    with open(filepath, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=all_headers)
        writer.writeheader()
        for ch in sorted(existing_rows):
            writer.writerow(existing_rows[ch])

