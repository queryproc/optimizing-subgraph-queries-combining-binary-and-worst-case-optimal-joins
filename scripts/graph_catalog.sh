#!/bin/bash

python3 change_snap_to_csv.py ./soc-Epinions1.txt ./output/data.csv
python3 serialize_dataset.py ./output/data.csv ./output/data
python3 serialize_catalog.py ./output/data