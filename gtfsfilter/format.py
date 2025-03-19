from pathlib import Path
import time
import logging
import datetime

def prepare_calendar(df_dict, output: Path):
    '''
    The r5py library requires all trips with unique services be mentioned in a "calendar" file. There are cases when This function is needed when exception-services are written only in a "calendar_dates" file by a modeller.
    Thus, the function merges exception-services from "calendar_dates" to "calendar", by reformating the datetime values into weekdays represented as numeric values. 
    Can be used if the gtfs file are clipped by one week maximum.
    '''
    
    if "calendar" in df_dict and "calendar_dates" in df_dict:
        output_dir = Path(output)
        t = time.time()
        dic = df_dict["calendar"].compute()
        service_ids = df_dict["calendar"]["service_id"].unique().compute()
        mask = (df_dict["calendar_dates"]["service_id"].isin(service_ids)==False)&(df_dict["calendar_dates"]["exception_type"]==1)
        for i, service_group in df_dict["calendar_dates"][mask].compute().groupby('service_id'):
            records = [0,0,0,0,0,0,0,service_group.date.min(),service_group.date.max(),i]
            for j, service in service_group.iterrows():
                day = datetime.datetime.strptime(str(service.date), '%Y%m%d').weekday()
                records[day]=1
            dic.loc[len(dic)] = records
        dic.to_csv(output_dir + "calendar.txt", index=False)
        del dic
        duration = time.time() - t
        logging.debug(f"Merged calendar dates with calendar for {duration:.2f}s")