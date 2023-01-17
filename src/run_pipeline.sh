#!/usr/bin/env bash
python3 merge_data.py &
python3 generate_plots.py &
python3 producers/weather.py & 
python3 producers/fuel_price.py & 
python3 producers/bike.py &