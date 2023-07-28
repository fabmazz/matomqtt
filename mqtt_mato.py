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


def on_message(mosq, obj, msg):
    mess=str(msg.payload,"utf-8")
    mm=json.loads(mess)
    _, line, veh = msg.topic.split("/")
    print(msg.topic + f" {line} {veh} " + str(msg.qos) + " "+str(mm) )
    posup = ups.make_update_json(mm, line, veh)

    ### add to session
    dbsess.add(posup)
    

    

def my_on_connect(mosq, obj, rc):
    global client
    print("WE HAVE CONNECTION")
    client.subscribe("10/#")

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")

client = Client("mtss-uiaoks-76719",transport="websockets")

#client.enable_logger(logger)
# Assign event callbacks
client.on_message = on_message
client.on_connect = on_connect 

client.ws_set_options(path="/scre",headers={"Origin": "https://mato.muoversiatorino.it","Host": "mapi.5t.torino.it", 
 })
 #"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
client.tls_set()

print("Prop",client._connect_properties)
client.connect("mapi.5t.torino.it",port=443,keepalive=60)
print("Connected")
#client.subscribe("/10/#")
#client.loop_start()
#client.loop_forever(timeout=5,retry_first_connection=True)

## create db

enginedb = create_engine(f"sqlite:///{DB_NAME}",future=True, echo=True)

ups.Base.metadata.create_all(enginedb)
dbsess = Session(enginedb)

client.loop_start()
try:
    while True:
        time.sleep(10)
        dbsess.commit()
finally:
    client.loop_stop()
    dbsess.close()
