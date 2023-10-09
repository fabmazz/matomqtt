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

class GtfsTrip(Base):
    __tablename__="gtfstrips"
    gtfsId = Column(String(15),nullable=False, primary_key=True)
    serviceId = Column(String(15),nullable=False)
    routeId = Column(String(15),nullable=False)
    patternCode = Column(String(15),nullable=False)
"""
class MatoPattern(Base):
    __tablename__ = "matopatterns"


 'serviceId': 'gtt:502422U',
 'route': {'gtfsId': 'gtt:38U'},
 'pattern': {'code': 'gtt:38U:0:04'},
 'wheelchairAccessible': 'POSSIBLE',
 'activeDates': ['20231007'],
 'tripShortName': None,
 'tripHeadsign': 'MIRAFIORI SUD, P.LE CAIO MARIO',
 'bikesAllowed': 'ALLOWED',
 'semanticHash': '3Ep15Q:ktfRcQ'}
"""  
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
def make_update_json(data, line, veh, time_r=None):
    return PosUpdate(line=line,veh=veh, 
        lat=float(data[0]),
        lon=float(data[1]),
        heading=clsorNone(int,data[2]),
        speed=clsorNone(int,data[3]),
        tripId = str(data[4]),
        direct = -5 if data[5] is None else int(data[5]),
        nextStop="-10" if data[6] is None else f"{data[6]}",
        full = int(data[7]) if len(data)>7 else -10,
        timerec = int(time.time()) if time_r is None else time_r
        )

def insert_update(dbupdate: PosUpdate,session: Session ):
    session.add(dbupdate)
