#!/usr/bin/env bash
python3 src/producers/weather.py & 
python3 src/producers/fuel_price.py & 
python3 src/producers/bike.py &