#!/bin/bash
#SBATCH --array=0-99  # Adjust based on the number of files
#SBATCH --time=01:00:00
#SBATCH --mem=4G
#SBATCH --output=slurm_output_%A_%a.out

# Load necessary modules
module load hdf5  # Load any necessary modules

# Read the file path from file_list.txt using the SLURM_ARRAY_TASK_ID
file_list=$(cat file_list.txt)
input_file=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" <<< "$file_list")

# If no input file found, exit
if [ -z "$input_file" ]; then
  echo "No input file found for task ID $SLURM_ARRAY_TASK_ID"
  exit 1
fi

# Run the Python script
python copy_h5_slurm.py "$input_file"
