#!/usr/bin/env bash
python3 merge_data.py &
python3 producers/bike.py &
python3 prodcuers/fuel_price.py & 