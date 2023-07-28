from sqlalchemy import Column,Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

import time

Base = declarative_base()

class PosUpdate(Base):
    __tablename__ = "pos_updates"

    id = Column(Integer,primary_key=True, autoincrement=True)
    line = Column(String(15),nullable=False)
    veh = Column(Integer, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    heading = Column(Integer)
    speed = Column(Integer)
    tripId=Column(String(30))
    direct = Column(Integer)
    nextStop=Column(String(15))
    full = Column(Integer)
    timerec = Column(Integer)

    def __repr__(self):
         return f"Veh {self.veh} line {self.line} @({self.lat},{self.lon}, {self.heading},{self.direct})"
    
    def __hash__(self):
        return hash((self.line,self.veh,self.lat,self.lon, self.tripId, self.nextStop))

    
"""
                parsedMessage = {
                    lat: compat[0],
                    long: compat[1],
                    hdg: compat[2],
                    spd: compat[3],
                    tripId: compat[4],
                    dir: compat[5],
                    nextStop: `gtt:${compat[6]}`, //riaggiungo prefisso gtt: strippato lato server
                    full: compat[7], // =0 vuoto. =1 pieno
                }
"""
clsorNone = lambda cls, inp : cls(inp) if inp is not None else None

## Convert update to data class
def make_update_json(data, line, veh):
    return PosUpdate(line=line,veh=veh, 
        lat=float(data[0]),
        lon=float(data[1]),
        heading=clsorNone(int,data[2]),
        speed=clsorNone(int,data[3]),
        tripId = str(data[4]),
        direct = -5 if data[5] is None else int(data[5]),
        nextStop="-10" if data[6] is None else f"{data[6]}",
        full = int(data[7]) if len(data)>7 else 0,
        timerec = int(time.time())
        )

def insert_update(dbupdate: PosUpdate,session: Session ):
    session.add(dbupdate)
