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
#import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import mysqlupds as ups

DB_NAME = "passaggi_mato.db"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

LIST_ADD = set() #[]

def on_message(mosq, obj, msg):
    global COUNT_ADD, LIST_ADD
    mess=str(msg.payload,"utf-8")
    mm=json.loads(mess)
    _, line, veh = msg.topic.split("/")
    #print(msg.topic + f" {line} {veh} " + str(msg.qos) + " "+str(mm) )
    #print(f"line {line} v {veh}, payload {mm}")
    posup = ups.make_update_json(mm, line, veh)

    ### add to session
    #dbsess.add(posup)
    LIST_ADD.add(posup)
    COUNT_ADD+=1
    

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

ups.Base.metadata.create_all(enginedb)
dbsess = Session(enginedb)

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
