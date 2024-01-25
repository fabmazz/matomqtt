#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 16:37:59 2023

@author: fabio
"""
import logging
import sys
import time
import json
import random
from threading import Lock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from paho.mqtt.client import Client

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import matoupdates.mysqlupds as ups
import matoupdates.pos_update as posups
import matoupdates.matolib as matolib
import matoupdates.iolib as iolib
import matoupdates.datescr as datelib #import get_name_datetime, make_basename_updates, timestamp

### CONSTANTS
MAX_UPS_FILE = 30_000

executor = ThreadPoolExecutor(2)
##globals
UPDATES_LOCK = Lock()
TRIPS_LOCK = Lock()
UPDATES_DOWNLOADED = []
HASH_UPS_DOWN = set()
COUNT_ADD = 0

FOLDER_SAVE = Path("ups_data")


make_DB_name = lambda: f"passaggi_mato_{datelib.get_name_datetime(datetime.now())}.db"
DB_NAME = make_DB_name()
UPDS_BASE_NAME= datelib.make_basename_updates()

PATTERNS_FNAME =Path(f"patterns_{datelib.get_name_datetime(datetime.now())}.json.zstd")

if(PATTERNS_FNAME.exists()):
    PATTERNS_DOWN = iolib.read_json_zstd(PATTERNS_FNAME)
else:
    print("No patterns file")
    PATTERNS_DOWN = {}

N_SAVED_PATTERNS = len(PATTERNS_DOWN)

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

handler = logging.StreamHandler(sys.stdout)
#handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

LIST_ADD = set() #[]
TRIPS_DOWN = []
DOWNLOADED_TRIPS = set()
format_date_sec = lambda date : f"{date.year}{date.month:02d}{date.day:02d}{date.hour:02d}{date.minute:02d}{date.second:02d}"

def download_patternInfo(patterncode):
    try:
        pattern = matolib.get_pattern_info(patterncode)

        code = pattern["code"]
        PATTERNS_DOWN[code] = pattern
    except Exception as e:
        print(f"Cannot download pattern {patterncode}, ex: {e}", file=sys.stderr)

def download_tripinfo(tripNumeric):
    
    gtfsid=f"gtt:{tripNumeric}U"
    if gtfsid=="gtt:NoneU":
        # in some cases the trip is a text 'None'
        return
    if gtfsid in DOWNLOADED_TRIPS:
        ## already downloaded
        return
    try:
        trip_d = matolib.get_trip_info(gtfsid)

        tripelm = ups.GtfsTrip(gtfsId=trip_d["gtfsId"], serviceId=trip_d["serviceId"],
                               routeId=trip_d["route"]["gtfsId"], patternCode=trip_d["pattern"]["code"])
        with TRIPS_LOCK:
            TRIPS_DOWN.append(tripelm)
            DOWNLOADED_TRIPS.add(gtfsid)

        patCode = tripelm.patternCode
        if(patCode not in PATTERNS_DOWN):
            executor.submit(download_patternInfo, patCode)
    except Exception as e:
        ### nothing work
        print(f"Download info for trip {tripNumeric}, type {type(tripNumeric)}, gtfsid: {gtfsid}")
        print(f"Failed to download data for trip {tripNumeric}, error: {e}",file=sys.stderr)

def on_message(mosq, obj, msg):
    global UPDATES_LOCK, UPDATES_DOWNLOADED, HASH_UPS_DOWN, COUNT_ADD
    mess=str(msg.payload,"utf-8")
    try:
        mm=json.loads(mess)
    
        _, line, veh = msg.topic.split("/")

        #posup = ups.make_update_json(mm, line, veh) # time_r=format_date_sec(nowt))
        posHash, posUpdate = posups.get_update_data(mm, line, veh)
        tripId = posUpdate["tripId"]
        if tripId is not None or tripId != "None":
                executor.submit(download_tripinfo, tripId)

        with UPDATES_LOCK:
            if posHash not in HASH_UPS_DOWN:
                HASH_UPS_DOWN.add(posHash)
                UPDATES_DOWNLOADED.append(posUpdate)
                COUNT_ADD+=1
        ### add to session
        #dbsess.add(posup)
        
        
     
    except Exception as e:
        print(f"An error happened during decoding at time {int(time.time())}, message is: \n\t{mess},\n\texception: {e}",
               file=sys.stderr)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")



csname = f'{random.randrange(16**6):06x}'
client = Client(f"mtss-ajino-{csname}",transport="websockets")

#client.enable_logger(logger)
# Assign event callbacks
client.on_message = on_message
client.on_connect = on_connect 

client.ws_set_options(path="/scre",headers={"Origin": "https://mato.muoversiatorino.it","Host": "mapi.5t.torino.it", 
 })
 #"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
client.tls_set()

print(f"Client id: {client._client_id}")
client.connect("mapi.5t.torino.it",port=443,keepalive=60)
#client.subscribe("/10/#")
#client.loop_start()
#client.loop_forever(timeout=5,retry_first_connection=True)

## create db
enginedb = create_engine(f"sqlite:///{DB_NAME}",future=True)
def start_db_session(engine):
    ups.Base.metadata.create_all(engine)
    return Session(engine)

dbsess = start_db_session(enginedb)

###generate name file
UPS_FILE = FOLDER_SAVE / datelib.ups_name_file(UPDS_BASE_NAME)

if not FOLDER_SAVE.exists():
    FOLDER_SAVE.mkdir(parents=True)

trips_pres = dbsess.scalars(select(ups.GtfsTrip)).all()
for tr in trips_pres:
    DOWNLOADED_TRIPS.add(tr.gtfsId)

client.loop_start()
prev_len = 0
try:
    while True:
        time.sleep(10)
        ### insert
        #print(f"Should have {COUNT_ADD} entries now")
        """if len(LIST_ADD) < 200:
            continue
        listadd=LIST_ADD
        LIST_ADD = set()
        """
        with UPDATES_LOCK:
            ## lock down
            nNew = len(UPDATES_DOWNLOADED) - prev_len 
            print(f"have {nNew} new updates")
            if nNew < 300:
                ## don't do anything
                continue
            
            prev_len = len(UPDATES_DOWNLOADED)
            ## save the data
            tt = time.time()
            iolib.save_json_zstd(UPS_FILE,UPDATES_DOWNLOADED, level=8)
            print(f"Saved {len(UPDATES_DOWNLOADED)} updates in {(time.time()-tt):4.3f} s")

            ## check if it is too many
            if len(UPDATES_DOWNLOADED) > MAX_UPS_FILE:
                ## change file name
                UPDATES_DOWNLOADED = []
                UPS_FILE = FOLDER_SAVE / datelib.ups_name_file(UPDS_BASE_NAME)
                prev_len = 0
            ## leave lock, updates are free
        
        # TRIPS
        with TRIPS_LOCK:
            trips_add = TRIPS_DOWN
            TRIPS_DOWN = list()
            dbsess.add_all(trips_add)
            dbsess.commit()
            ## leave lock
        
        if len(PATTERNS_DOWN) > N_SAVED_PATTERNS:
            patterns_down = dict(PATTERNS_DOWN)
            print(f"Save {len(patterns_down)} patterns")
            saved = iolib.save_json_zstd(PATTERNS_FNAME, patterns_down, level=10)
            if saved: 
                N_SAVED_PATTERNS = len(patterns_down)
        #print(f"inserting {len(listadd)} updates - {int(time.time())}")
        #dbsess.add_all(listadd)


        ### check if to cut database
        if make_DB_name() != DB_NAME:
            ##close DB
            dbsess.close()
            DB_NAME = make_DB_name()
            print(f"Changing DB, new name: {DB_NAME}")
            enginedb = create_engine(f"sqlite:///{DB_NAME}",future=True)
            dbsess = start_db_session(enginedb)
            PATTERNS_FNAME =Path(f"patterns_{datelib.get_name_datetime(datetime.now())}.json.zstd")
            UPDS_BASE_NAME = datelib.make_basename_updates()
            UPS_FILE = FOLDER_SAVE / datelib.ups_name_file(UPDS_BASE_NAME)
            PATTERNS_DOWN = {}
            N_SAVED_PATTERNS = 0
        
        COUNT_ADD = 0
except Exception as e: 
    print("Exception happened: ",e)
finally:
    print("Finish operation, save data and close DB")
    client.loop_stop()
    #listadd=LIST_ADD
    #LIST_ADD = set()
    with TRIPS_LOCK:
        trips_add = TRIPS_DOWN
        TRIPS_DOWN = list()
    dbsess.add_all(trips_add)
    #print(f"list add has {len(listadd)} items")
    #dbsess.add_all(listadd)
    
    iolib.save_json_zstd(PATTERNS_FNAME, PATTERNS_DOWN, level=10)
    with UPDATES_LOCK:
        ## wait for threads to stop
        tt = time.time()
        iolib.save_json_zstd(UPS_FILE,UPDATES_DOWNLOADED)
        print(f"Saved updates in {(time.time()-tt):4.3f} s")
    dbsess.commit()
    dbsess.close()
    executor.shutdown()
