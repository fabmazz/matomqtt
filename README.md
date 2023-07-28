# Collector of position updates from Muoversi A Torino (MATO)

This set of scripts can be used to download the raw positions from the Muoversi a Torino website [[1]][mato].
These positions can be viewed in the map on the website, and use MQTT under the hood.

To run the script, you need the python packages `paho-mqtt` and `sqlalchemy` (old version, 1.4)

Run `mato_mqtt.py` to download the positions and save them in a sqlite database.

[mato]: http://www.muoversiatorino.it