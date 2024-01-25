from collections import namedtuple
import time
from datetime import datetime

PosUpdate = namedtuple("PosUpdate",["line","veh","lat","lon", "heading","speed",
                                    "tripId", "direct","nextStop","full","timerec"])


clsorNone = lambda cls, inp : cls(inp) if inp is not None else None
classorValue = lambda cls, inp, def_val : def_val if inp is None else cls(inp) 

def get_update_data(data, line, veh, time_r=None):
    data = dict(line=line,veh=veh, 
        lat=float(data[0]),
        lon=float(data[1]),
        heading=classorValue(int,data[2], -2000),
        speed=classorValue(int,data[3], -2000),
        tripId = str(data[4]) if data[4] is not None else "None",
        direct = -5 if data[5] is None else int(data[5]),
        nextStop="-10" if data[6] is None else f"{data[6]}",
        full = int(data[7]) if len(data)>7 else -10,
        timerec = int(time.time()) if time_r is None else time_r
        )
    
    
    mhash =  hash((data["line"], data["veh"], data["lat"], data["lon"], data["tripId"], data["timerec"]))
    return mhash, data