#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

# Input file from the command line argument
input_file=$1

# Check if the input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: File '$input_file' not found!"
    exit 1
fi

# Hardcoded output directory
output_dir="/gws/nopw/j04/ncas_radar_vol3/ukmo-nimrod/biorad"

# Check if output directory exists, if not, create it
if [ ! -d "$output_dir" ]; then
    echo "Creating output directory: $output_dir"
    mkdir -p "$output_dir"
fi

# Print the input file being processed
echo "Processing input file: $input_file"

# Run the Python script with the specified input file and hardcoded output directory
python ukmo2bioradinput.py "$input_file" "$output_dir"
