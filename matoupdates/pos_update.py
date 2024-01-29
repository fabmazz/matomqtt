from collections import namedtuple
import time
from datetime import datetime
from . import posupdate_pb2 as posup

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
    pbmess = process_pos(data)
    
    mhash =  hash((data["line"], data["veh"], data["lat"], data["lon"], data["tripId"], data["timerec"]))
    return mhash, pbmess

def process_pos(ex):
    pbmess = posup.PositionUpdate()
    pbmess.line = ex["line"]
    pbmess.veh = ex["veh"]
    pbmess.lat = ex["lat"]
    pbmess.lon = ex["lon"]
    pbmess.heading = ex["heading"]
    pbmess.speed = ex["speed"]
    pbmess.tripId = ex["tripId"]
    pbmess.direction = ex["direct"]
    pbmess.nextStop = ex["nextStop"]
    pbmess.full = ex["full"]
    pbmess.timerec = ex["timerec"]
    return pbmess