#!/usr/bin/env bash
python3 producers/weather.py & 
python3 producers/fuel_price.py & 
python3 producers/bike.py &