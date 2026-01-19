#!/bin/bash
#SBATCH --array=0-3200  # Adjust based on the number of files (e.g., 100 files)
#SBATCH --time=00:10:00
#SBATCH --mem=4G
#SBATCH --job-name=chenies2biorad
#SBATCH --partition=short-serial 
#SBATCH --output=slurm_output_%A_%a.out

# Hardcoded output directory
output_dir="/gws/nopw/j04/ncas_radar_vol3/ukmo-nimrod/biorad"

# Check if output directory exists, if not, create it
if [ ! -d "$output_dir" ]; then
    echo "Creating output directory: $output_dir"
    mkdir -p "$output_dir"
fi

# Read the file path from file_list.txt using the SLURM_ARRAY_TASK_ID
file_list=$(cat file_list.txt)
input_file=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" <<< "$file_list")

# If no input file found, exit
if [ -z "$input_file" ]; then
  echo "No input file found for task ID $SLURM_ARRAY_TASK_ID"
  exit 1
fi

# Run the Python script with the input file and hardcoded output directory
conda activate pyart_3_8_radar_group
python ukmo2bioradinput.py "$input_file" "$output_dir"
