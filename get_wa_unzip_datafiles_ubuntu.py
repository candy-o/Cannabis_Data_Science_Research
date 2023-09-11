#  `get_wa_unzip_datafiles_ubuntu.py` 
# 
#  `curate_ccrs_lab_results() unzip_datafiles(data_dir) extracts WA State CCRS zip file as: 
#     /mnt/HDD/data/washington/CCRS_PRR_8-4-23/CCRS PRR (8-4-23)/LabResult_0/LabResult_0\LabResult_0.csv"
#   resulting in "FileNotFoundErrors" running `get_results_wa.py`

    # Comment out this line of code `get_results_wa.py` curate_ccrs_lab_results()
    # unzip_datafiles(data_dir)
    # then run `get_results_wa_unzip_datafiles_ubuntu.py` previous to `get_results_wa.py`
    
# This script extracts Washington State CCRS directory structure to an Ubuntu friendly format.
# 
# Example:
# From: /mnt/HDD/data/washington/CCRS_PRR_8-4-23/CCRS PRR (8-4-23)/LabResult_0/LabResult_0\LabResult_0.csv"
# To:   /mnt/HDD/data/washington/CCRS_PRR_8-4-23/CCRS_PRR_8-4-2/LabResult_0/LabResult_0/LabResult_0.csv
# Candace O'Sullivan (Sutherland)
# 9/11/2023

# # Standard imports:
import os
import zipfile
import glob

base = '/mnt/HDD/data/washington'
dir_path = f'{base}/CCRS_PRR_8-4-23/'
extracted_path = "/mnt/HDD/data/washington/CCRS_PRR_8-4-23/CCRS PRR (8-4-23)"
new_path = "/mnt/HDD/data/washington/CCRS_PRR_8-4-23/CCRS_PRR_8-4-23"

file = 'CCRS_PRR_(8-4-23).zip'

# copy CCRS_PRR_(8-4-23).zip to base dir
os.chdir(base)

# Unzip the file to the new directory
zip_path = os.path.join(base, file)
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(dir_path)
                
os.rename(extracted_path, new_path)

os.chdir(new_path)

print(os.getcwd())

# List all files in the current directory

current_directory = os.getcwd()
files = os.listdir(current_directory)

for file in files:
    # Only proceed if it's a zip file
    if file.endswith('.zip'):
        try:
            # Strip the .zip from the filename
            layer1_dir_name = file[:-4]
            dir_name = f'{layer1_dir_name}/{layer1_dir_name}'
            # Create a new directory
            
            new_dir_path = os.path.join(current_directory, dir_name)
            os.makedirs(new_dir_path, exist_ok=True)

            # Unzip the file to the new directory
            zip_path = os.path.join(current_directory, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(new_dir_path)
            
            # Iterate through the files in the new directory
            for root, dirs, filenames in os.walk(new_dir_path):
                for filename in filenames:
                    try:
                        old_path = os.path.join(root, filename)
                        
                        # Split the filename using the delimiter and take the part after the delimiter
                        new_filename = filename.split("\\")[-1]
                        
                        new_path = os.path.join(root, new_filename)
                        
                        # Rename the file
                        os.rename(old_path, new_path)
                        
                        # print("Filename...", filename)
                        # print("File: ", file)
                        
                        os.remove(file)

                    except Exception as e:
                        print(f"An error occurred while renaming {filename}: {e}")
                        
        except Exception as e:
            print(f"An error occurred while processing {file}: {e}")
            # An error occurred while processing SaleHeader_42.zip: File is not a zip file
            
            
        




