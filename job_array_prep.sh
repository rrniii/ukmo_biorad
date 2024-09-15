#!/bin/bash

# Directory where all the files are stored
base_dir="/gws/nopw/j04/ncas_radar_vol3/ukmo-nimrod/raw_h5_data/single-site/chenies"

# Create a file list (one file path per line)
find ${base_dir} -type f -name "*_polar_pl_radar05_aggregate.h5" > file_list.txt

