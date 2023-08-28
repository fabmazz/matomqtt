#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 16:37:59 2023

@author: fabio
"""
from paho.mqtt.client import Client
import logging
import sys
import time
import json
import random
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import mysqlupds as ups
from datescr import get_name_datetime

make_DB_name = lambda: f"passaggi_mato_{get_name_datetime(datetime.now())}.db"
DB_NAME = make_DB_name()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

LIST_ADD = set() #[]
format_date_sec = lambda date : f"{date.year}{date.month:02d}{date.day:02d}{date.hour:02d}{date.minute:02d}{date.second:02d}"


def on_message(mosq, obj, msg):
    global COUNT_ADD, LIST_ADD
    mess=str(msg.payload,"utf-8")
    try:
        mm=json.loads(mess)
    
        _, line, veh = msg.topic.split("/")
        #print(msg.topic + f" {line} {veh} " + str(msg.qos) + " "+str(mm) )
        #print(f"line {line} v {veh}, payload {mm}")
        #nowt = datetime.now()
        posup = ups.make_update_json(mm, line, veh) # time_r=format_date_sec(nowt))
        
        ### add to session
        #dbsess.add(posup)
        LIST_ADD.add(posup)
        COUNT_ADD+=1
     
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
COUNT_ADD = 0
enginedb = create_engine(f"sqlite:///{DB_NAME}",future=True)
def start_db_session(engine):
    ups.Base.metadata.create_all(engine)
    return Session(engine)

dbsess = start_db_session(enginedb)

client.loop_start()
try:
    while True:
        time.sleep(10)
        ### insert
        print(f"Should have {COUNT_ADD} entries now")
        if len(LIST_ADD) < 200:
            continue
        listadd=LIST_ADD
        LIST_ADD = set()
        print(f"inserting {len(listadd)} updates - {int(time.time())}")
        dbsess.add_all(listadd)
        dbsess.commit()
        ### check if to cut database
        if make_DB_name() != DB_NAME:
            ##close DB
            dbsess.close()
            DB_NAME = make_DB_name()
            print(f"Changing DB, new name: {DB_NAME}")
            enginedb = create_engine(f"sqlite:///{DB_NAME}",future=True)
            dbsess = start_db_session(enginedb)
        
        COUNT_ADD = 0
except Exception as e: 
    print("Exception happened: ",e)
finally:
    print("Close DB")
    client.loop_stop()
    listadd=LIST_ADD
    LIST_ADD = set()
    print(f"list add has {len(listadd)} items")
    dbsess.add_all(listadd)
    dbsess.commit()
    dbsess.close()
