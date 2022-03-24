# 'SQLiteCreate_Database.py' converts individual csv files (456.6GB) to individual SQLite3 databases (255.2GB)
# Author: Candace O'Sullivan-Sutherland 3/21/2022 (Python 3.8.5)
# 
# The Washington State November2021_Tracability_Reports Dataset consist of 28 csv files totaling 456.6GB
# Pandas loads the complete dataset in memory (unless data is augmented) and is best used with < 1M datasets.
# SQLite3 stores the datasets(data persists) and we can use SQL to reduce the data needed to load into Pandas.
#
# License GPLv3+: GNU GPL version 3 or later https://gnu.org/licenses/gpl.html 
# This is free software: you are free to change and redistribute it. 
# There is NO WARRANTY, to the extent permitted by law.

# Washington State Cannabis dataset csv files: (first we process files < 10GB then files => 10GB) 
 
# list of csv files under 10GB

# listDatasetsUnder10GB = [ "Areas_0.csv", "LabResults_0.csv", "LabResults_1.csv", "LabResults_2.csv", 
#                           "LabRetests_0.csv", "Licensees_0.csv", "MmeUser_0.csv", "Strains_0.csv", 
#                           "Taxes_0.csv", "Users_0.csv"]

# Areas_0.csv (83.4MB)
# LabResults_0.csv (1.2GB)
# LabResults_1.csv (1.1GB)
# LabResults_2.csv (138MB)
# LabRetests_0.csv (288 bytes)
# Licensees_0.csv (1.3MB)
# MmeUser_0.csv (5.8MB)
# Strains_0.csv (270.1MB)
# Taxes_0.csv (130 bytes)
# Users_0.csv (7.1MB)


# list of csv files over 10GB (Read in Chunks)

# listDatasetsOver10GB = [ "Batches_0.csv", "Disposals_0.csv", "Inventories_0.csv", "InventoryAdjustments_0.csv", 
#                          "InventoryAdjustments_1.csv", "InventoryAdjustments_2.csv", "InventoryTypes_0.csv",  
#                          "Plants_0.csv", "SaleItems_0.csv", "SaleItems_1.csv", "SaleItems_2.csv",
#                          "SaleItems_3.csv", "Sales_0.csv", "Sales_1.csv", "Sales_2.csv"]

# Batches_0.csv (27.7GB)
# Disposals_0.csv (10.2GB)
# Inventories_0.csv (32.2GB)
# InventoryAdjustments_0.csv (24.9GB)
# InventoryAdjustments_1.csv (24.6GB)
# InventoryAdjustments_2.csv (22.8GB)
# InventoryTransferItems_0.csv (15.6GB)
# InventoryTypes_0.csv (15.2GB)
# Plants_0.csv (13.2GB)
# SaleItems_0.csv (43.7GB)
# SaleItems_1.csv (49.3GB)
# SaleItems_2.csv (49.6GB)
# SaleItems_3.csv (42.9GB)
# Sales_0.csv (31.9GB)
# Sales_1.csv (38.0GB)
# Sales_2.csv (11.9GB)

# Import Modules
import os
import gc
import time
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# Begin: Program runtime
start_time = time.perf_counter()
from humanfriendly import format_timespan
begin_time = time.time()

# set directory where dataset csv files are read from
CSV_DATA_DIR_IN = "/mnt/HDD"

# set directory where dataset db files are written to
DB_DATA_DIR_OUT = "/mnt/USB12TB/WashingtonStateDatabases"

# using DB_DATA_DIR_OUT (where databases are written) to create SQLite database files
areas_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/areas.db")
labresults_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/labresults.db")
labretests_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/labretests.db")
licensees_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/licenses.db")
mmeuser_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/mmeuser.db")
strains_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/strains.db")
taxes_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/taxes.db")
users_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/users.db")
   
# Start with files < 10GB

print("BEGIN: Files under 10GB.....................")

listDatasetsUnder10GB = [ "Areas_0.csv", "LabResults_0.csv", "LabResults_1.csv", "LabResults_2.csv", 
                          "LabRetests_0.csv", "Licensees_0.csv", "MmeUser_0.csv", "Strains_0.csv", 
                          "Taxes_0.csv", "Users_0.csv" ]

# Areas_0.csv (83.4MB)
# LabResults_0.csv (1.2GB)
# LabResults_1.csv (1.1GB)
# LabResults_2.csv (138MB)
# LabRetests_0.csv (288 bytes)
# Licensees_0.csv (1.3MB)
# MmeUser_0.csv (5.8MB)
# Strains_0.csv (270.1MB)
# Taxes_0.csv (130 bytes)
# Users_0.csv (7.1MB)

for file_name in listDatasetsUnder10GB:
    
    DATA_FILE_IN = f'{CSV_DATA_DIR_IN}/{file_name}' 
    
    print(f'READ CSV filename:', file_name)
    
    data = pd.read_csv(f'{DATA_FILE_IN}', error_bad_lines=False,
            sep='\t',
            encoding='utf-16')
    
    data = data.rename(columns = {c: c.replace(' ', '') for c in data.columns})
    
    # passing {file} as table name does not work
    # You cannot use a tablename as a query parameter to prevent that data being interpreted as an object name.
    # https://stackoverflow.com/questions/25014915/how-to-use-filenames-from-csv-files-as-table-names-in-sqlite
    # I used brute force if-elif statement instead of string interpolation 

    if   file_name == 'Areas_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('areas', areas_database, if_exists = 'append')   
    elif file_name  == 'LabResults_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('labresults', labresults_database, if_exists = 'append') 
    elif file_name  == 'LabResults_1.csv':
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('labresults', labresults_database, if_exists = 'append') 
    elif file_name  == 'LabResults_2.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('labresults', labresults_database, if_exists = 'append') 
    elif file_name  == 'LabRetests_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('labretests', labretests_database, if_exists = 'append') 
    elif file_name  == 'Licensees_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('licenses', licensees_database, if_exists = 'append') 
    elif file_name  == 'MmeUser_0.csv': 
         print(f'WRITING to SQLite DB using file::', file_name)
         data.to_sql('mmeuser', mmeuser_database, if_exists = 'append') 
    elif file_name  == 'Strains_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('strains', strains_database, if_exists = 'append') 
    elif file_name  == 'Taxes_0.csv': 
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('taxes', taxes_database, if_exists = 'append')          
    elif file_name  == 'Users_0.csv':
         print(f'WRITING to SQLite DB using file:', file_name)
         data.to_sql('users', users_database, if_exists = 'append') 
    else:
         print(f'ELSE HIT: Working with filename:', file_name)
         
     
print("END:   Files under 10GB...................")

print("....................................................")
print("BEGIN: Files over  10GB.............................")
    

# chunk files > 10GB

listDatasetsOver10GB = [ "Batches_0.csv", "Disposals_0.csv", "Inventories_0.csv", "InventoryAdjustments_0.csv", 
                          "InventoryAdjustments_1.csv", "InventoryAdjustments_2.csv", "InventoryTypes_0.csv",  
                          "Plants_0.csv", "SaleItems_0.csv", "SaleItems_1.csv", "SaleItems_2.csv", "SaleItems_3.csv", 
                          "Sales_0.csv", "Sales_1.csv", "Sales_2.csv"]

# using DB_DATA_DIR_OUT (where databases are written) to create SQLite database files
batches_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/batches.db")
disposals_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/disposals.db")
inventories_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/inventories.db")
inventoryadjustments_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/inventoryadjustments.db")
inventorytypes_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/inventorytypes.db")
plants_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/plants.db")
saleitems_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/saleitems.db")
sales_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT) + "/sales.db")

                        
for file_name in listDatasetsOver10GB:
    
    print(f'Working with filename:', file_name)
    
    DATA_FILE_IN = f'{CSV_DATA_DIR_IN}/{file_name}' 
    
    chunksize = 1000000
    i = 0
    j = 0
    print('| index: {}'.format(j))
        
    for chunk_df in pd.read_csv(f'{DATA_FILE_IN}', chunksize=chunksize, error_bad_lines=False, 
                    iterator=True, sep='\t', encoding='utf-16'):
    
        chunk_df = chunk_df.rename(columns = {c: c.replace(' ', '') for c in chunk_df.columns})
        chunk_df.index += j 
        
        # passing {file} as table name did not work
        # brute force if-elif statement used 
   
        if   file_name == 'Batches_0.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('batches', batches_database, if_exists = 'append') 
        elif file_name  == 'Disposals_0.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('disposals', disposals_database, if_exists = 'append') 
        elif file_name  == 'Inventories_0.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('inventories', inventories_database, if_exists = 'append')    
        elif file_name  == 'InventoryAdjustments_0.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('inventoryadjustments', inventoryadjustments_database, if_exists = 'append') 
        elif file_name  == 'InventoryAdjustments_1.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('inventoryadjustments', inventoryadjustments_database, if_exists = 'append') 
        elif file_name  == 'InventoryAdjustments_2.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('inventoryadjustments', inventoryadjustments_database, if_exists = 'append') 
        elif file_name  == 'InventoryTypes_0.csv':         
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('inventorytypes', inventorytypes_database, if_exists = 'append') 
        elif file_name  == 'Plants_0.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('plants', plants_database, if_exists = 'append')             
        elif file_name  == 'SaleItems_0.csv':
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('saleitems', saleitems_database, if_exists = 'append')                   
        elif file_name  == 'SaleItems_1.csv':
            print(f'WRITING to SQLite DB using file:', file_name) 
            chunk_df.to_sql('saleitems', saleitems_database, if_exists = 'append')                            
        elif file_name  == 'SaleItems_2.csv':
            print(f'WRITING to SQLite DB using file:', file_name) 
            chunk_df.to_sql('saleitems', saleitems_database, if_exists = 'append')  
        elif file_name  == 'SaleItems_3.csv':
            print(f'WRITING to SQLite DB using file:', file_name)                             
            chunk_df.to_sql('saleitems', saleitems_database, if_exists = 'append')           
        elif file_name  == 'Sales_0.csv':
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('sales', sales_database, if_exists = 'append') 
        elif file_name  == 'Sales_1.csv':
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('sales', sales_database, if_exists = 'append') 
        elif file_name  == 'Sales_2.csv': 
            print(f'WRITING to SQLite DB using file:', file_name)
            chunk_df.to_sql('sales', sales_database, if_exists = 'append') 
        else:
            print(f'ELSE HIT: Working with filename:', file_name)
            
        j = chunk_df.index[-1]+1
        print('| index: {}'.format(j))

print("END:   Files over 10GB....................")
  
# End: Program runtime  
end_time = time.perf_counter()
print("Performance Counter", end_time - start_time, "seconds")
end_time = time.time() - begin_time
print("Total execution time: ", format_timespan(end_time))
    
# Test out your SQLite database
# Connect to SQLite database
# washingtonstate_database = create_engine("sqlite:///" + os.path.abspath(DB_DATA_DIR_OUT))
# conn = sqlite3.connect(DB_DATA_DIR_OUT)
# c = conn.cursor()
# c.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
# c.execute("SELECT * FROM saleitems")
# print(c.fetchmany(30000))
# conn.commit()   
# conn.close()   
