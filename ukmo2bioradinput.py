import h5py
import os
import sys

def copy_h5_data(input_file, output_dir):
    # Open the existing H5 file in read mode
    with h5py.File(input_file, 'r') as h5file:
        # Check if the 'lp' group exists
        if 'lp' not in h5file:
            print(f"'lp' group not found in {input_file}")
            return
        
        # Get all keys in the 'lp' group
        lp_keys = list(h5file['lp'].keys())
        print(f"Found {len(lp_keys)} keys in 'lp' group: {lp_keys}")

        # Extract the base name of the input file (without extension)
        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # Loop through each key in 'lp' and copy the datasets
        for key in lp_keys:
            group = h5file[f'lp/{key}']
            print(f"Copying datasets from 'lp/{key}'...")

            # Define the output file name by appending the group name to the base name
            output_file = os.path.join(output_dir, f"{base_name}_{key}.h5")
            print(f"Saving output to '{output_file}'")

            # Open the output H5 file in write mode
            with h5py.File(output_file, 'w') as new_h5file:
                # Function to recursively copy datasets to the root level
                def copy_datasets_to_root(group, base_name=""):
                    for item_name, item in group.items():
                        print('name is:' + item_name)
                        print(item)
                        full_item_name = f"{base_name}{item_name}"
                        if isinstance(item, h5py.Dataset):
                            # Copy dataset to the root of the new file
                            new_h5file.create_dataset(full_item_name, data=item[:])
                        elif isinstance(item, h5py.Group):
                            if item_name in ['where', 'what']:
                                h5file.copy(h5file[f'lp/{full_item_name}'], new_h5file, name=full_item_name)
                            else:
                                copy_datasets_to_root(item, base_name=f"{full_item_name}/")

                # Start copying datasets for this specific group
                copy_datasets_to_root(group, base_name=f"{key}/")

    print(f"All datasets from 'lp' group keys have been saved with the corresponding group name in the output file.")

if __name__ == "__main__":
    # Read input file path and output directory from command line
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    # Call the function
    copy_h5_data(input_file, output_dir)
