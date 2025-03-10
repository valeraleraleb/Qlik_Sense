# совместно с https://github.com/GrowShip
import argparse
import pandas as pd
import xml.etree.ElementTree as ET
from pyqvd import QvdTable
from datetime import datetime
from pathlib import Path

__log_dir__ = None

def set_log_dir(path_to):
    global __log_dir__
    __log_dir__ = Path(path_to).parent
    
def append_log(text):
    log_file = __log_dir__ / "update.log"
    with log_file.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def main(path_from, path_to):
    set_log_dir(path_to)
    
    append_log(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} | Transforming started | {path_from}")
    
    source_qvd = QvdTable.from_qvd(path_from)
    # source_qvd = QvdTable.from_qvd(r"C:\Qlik Server Space\Extract\1С ШР\КодШР\main_data_[name].qvd")
    source_df = source_qvd.to_pandas()
    source_df = source_df.reset_index()
    del source_qvd
    
    transformed_df = pd.DataFrame()
    
    for index, row in source_df.iterrows():
        source_oneRowXML = ET.fromstring(row['xml_row'])
        
        oneLineColumns = []
        oneLineData = []
        # .//ns0:
        for column in source_oneRowXML.findall('{http://v8.1c.ru/8.1/data/core}column'):
            for item in column.findall('{http://v8.1c.ru/8.1/data/core}Name'):
                oneLineColumns.append(item.text)
        
        for row in source_oneRowXML.findall('{http://v8.1c.ru/8.1/data/core}row'):
            one_row = []
            for child in row:
                one_row.append(child.text)
            oneLineData.append(one_row)
            
        transformed_df = pd.concat([transformed_df, pd.DataFrame(oneLineData, columns = oneLineColumns)])
        
    append_log(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} | Creating QVD | {path_to}")
    
    transformed_qvd = QvdTable.from_pandas(transformed_df)
    # name = .. or ..
    # print("Saving QVD",datetime.now())
    transformed_qvd.to_qvd(path_to)
    # transformed_qvd.to_qvd(r"C:\Qlik Server Space\#Test\....qvd")
    
    append_log(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} | Saved QVD | {path_to}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Args for script")
    parser.add_argument("path_from", type=str, help="path from")
    parser.add_argument("path_to", type=str, help="path to")
    
    args = parser.parse_args()
    main(args.path_from, args.path_to)
    
