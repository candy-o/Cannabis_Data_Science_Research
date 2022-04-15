# `CountAndPlotNumberNaNsWS2022.py` 
# Author: Candace O'Sullivan-Sutherland 4/14/2022 https://github.com/candy-o
#
# License GPLv3+: GNU GPL version 3 or later https://gnu.org/licenses/gpl.html 
# This is free software: you are free to change and redistribute it. 
# There is NO WARRANTY, to the extent permitted by law.
# Washington State Cannabis dataset csv files 
# https://lcb.app.box.com/s/e89t59s0yb558tjoncjsid710oirqbgd?page=1
# https://lcb.app.box.com/s/e89t59s0yb558tjoncjsid710oirqbgd?page=2
# Python Script reads datasets from csv, counts the number of NaNs per table attribute, saves to sumNaN csv, then plots

# Import Modules
import os
import gc
import time
import pandas as pd
import numpy as np
from re import search
from pathlib import Path
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.options.display.float_format = "{:.2f}".format


# Begin: Program runtime
start_time = time.perf_counter()
from humanfriendly import format_timespan
begin_time = time.time()

# set dir where dataset csv files are read from
CSV_DATA_DIR_IN = '/mnt/USB12TB/WashingtonStateCannabisDatasets/3-12-2022/Original_Data/csv_files'

# set dir where sumNaN csv files are written
CSV_DATA_DIR_OUT = "/mnt/USB12TB/testing"

# set dir to read sumNaN csv files 
sumNaN_CSV_DATA_DIR_IN = '/mnt/USB12TB/testing'

# set dir_out where plot png files go
sumNaN_CSV_DATA_DIR_OUT = "/mnt/USB12TB/testing"

# list of the csv files to sumNaN
csv_files = [ "Areas_0.csv", "Contacts_0.csv", "Integrator_0.csv", "Inventory_0.csv", "InventoryAdjustment_0.csv",
             "InventoryPlantTransfer_0.csv", "LabResult_0.csv", "Licensee_0.csv", "Plant_0.csv", "PlantDestructions_0.csv",
             "Product_0.csv", "SaleHeader_0.csv", "SalesDetail_0.csv", "Strains_0.csv" ]

# list of the expected sumNaN csv files 
sumNaN_csv_files = [ "sumNaNAreas_0.csv", "sumNaNContacts_0.csv", "sumNaNIntegrator_0.csv", "sumNaNInventory_0.csv", 
                    "sumNaNInventoryAdjustment_0.csv", "sumNaNInventoryPlantTransfer_0.csv", "sumNaNLabResult_0.csv", 
                    "sumNaNLicensee_0.csv", "sumNaNPlant_0.csv", "sumNaNPlantDestructions_0.csv",
                    "sumNaNProduct_0.csv", "sumNaNSaleHeader_0.csv", "sumNaNSalesDetail_0.csv", "sumNaNStrains_0.csv" ]

InventoryAdjustment_count = 0
LabResults_count = 0
SaleItems_count = 0
Sales_count = 0          

# sum the csv file NaNs to sumNaN{file_name}.csv

for file_name in csv_files:
    
    chunksize = 1000000
    chunk_df_series_sum = 0
    row_count = 0
    j = 0
    
    DATA_FILE_IN = f'{CSV_DATA_DIR_IN}/{file_name}' 
    
    print(f'READ CSV filename:', file_name)
    
    print(f'Working with filename:', file_name)
    
    DATA_FILE_IN  = f'{CSV_DATA_DIR_IN}/{file_name}' 
    DATA_FILE_OUT = f'{CSV_DATA_DIR_OUT}/{file_name}' 
        
    for chunk_df in pd.read_csv(f'{DATA_FILE_IN}', chunksize=chunksize, error_bad_lines=False, 
 #                   iterator=True, sep='\t', encoding='utf-16', index_col=None, header=0 ):
                   iterator=True, sep=',', encoding='latin-1', engine='python', index_col=None, 
                   header=0, quotechar='"', quoting=3):        
        row_count += len(chunk_df)

        chunk_df_series_sum += (chunk_df.isna().sum())

        chunk_df.index += j         
        j = chunk_df.index[-1]+1
        print('| index: {}'.format(j))
        num_rows = print('| index: {}'.format(row_count))            

        series_DATA_FILE_OUT = f'{CSV_DATA_DIR_OUT}/sumNaN{file_name}' 

        numpy_array = np.array([row_count])
        seri = pd.Series(data=numpy_array, index=['row_count'])

        chunk_df_series_sum.to_csv(f'{series_DATA_FILE_OUT}', sep='\t', header = 'True', index = 'False' )
        seri.to_csv(f'{series_DATA_FILE_OUT}', sep='\t', header = 'True', index = 'False', mode='a'  )
  

# Read sumNaN csv files here, sum the multiple files[0-3], then plot

for file_name in sumNaN_csv_files:
    
    chunk_df_series_sum = 0
    
    DATA_FILE_IN  = f'{sumNaN_CSV_DATA_DIR_IN}/{file_name}' 
    DATA_FILE_OUT = f'{sumNaN_CSV_DATA_DIR_OUT}/{file_name}' 
        
    for chunk1_df in pd.read_csv(f'{DATA_FILE_IN}', error_bad_lines=False, 
                    iterator=True, sep='\t', encoding='utf-8', index_col=None, header=None ):
         
        # Drop the rows with NaN picked up from df.to_csv() in previous script
        chunk_df = chunk1_df.dropna()

        fullstring = file_name
        
        substring = "Areas_"
        if search(substring, fullstring):
            sumNaNAreas_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNAreas_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Areas dataset # of NaNs")
                  
        substring = "Contacts_"
        if search(substring, fullstring):
            sumNaNContacts_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNContacts_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Contacts dataset # of NaNs")

        substring = "Integrator_"
        if search(substring, fullstring):
            sumNaNIntegrator_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNIntegrator_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Integrator dataset # of NaNs")
                    
        substring = "Inventory_"
        if search(substring, fullstring):
            sumNaNInventory_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNInventory_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Inventory dataset # of NaNs")
            
        substring = "InventoryAdjustment_"
        if search(substring, fullstring):
            sumNaNInventoryAdjustment_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNInventoryAdjustment_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Inventory Adjustment dataset # of NaNs")
                       
        substring = "InventoryPlantTransfer_"
        if search(substring, fullstring):
            sumNaNInventoryPlantTransfer_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNInventoryPlantTransfer_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Inventory Plant Transfer dataset # of NaNs")
            
        substring = "LabResult_"
        if search(substring, fullstring):
            sumNaNLabResult_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNLabResult_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("LabResult dataset # of NaNs")
  
        substring = "Licensee_"
        if search(substring, fullstring):
            sumNaNLicensee_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNLicensee_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Licensee dataset # of NaNs")

        substring = "Plant_"
        if search(substring, fullstring):
            sumNaNPlant_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNPlant_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Plant dataset # of NaNs")
        
        substring = "PlantDestructions_"
        if search(substring, fullstring):
            sumNaNPlant_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNPlant_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Plant Destructions dataset # of NaNs")
            
        substring = "Product_"
        if search(substring, fullstring):
            sumNaNProduct_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNProduct_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Product dataset # of NaNs")
            
                        
        substring = "SaleHeader_"
        if search(substring, fullstring):
            sumNaNSaleHeader_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNSaleHeader_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Sale Header dataset # of NaNs")
            
                                
        substring = "SalesDetail_"
        if search(substring, fullstring):
            sumNaNSalesDetail_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNSalesDetail_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Sales Detail dataset # of NaNs")
                        
        substring = "Strains"
        if search(substring, fullstring):
            sumNaNStrains_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNStrains_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Strains dataset # of NaNs")
        
 
            
        if plotMe == 1:
                
            Sorted = SortedAll[SortedAll.iloc[:, 1] >=1]
            x = list(Sorted[0])
            y = list(Sorted[1])
            ax.set_position([1,1,1,1])
            plt.xscale("log")
            plt.ticklabel_format(axis='y',style='plain')
            fig = plt.figure()
            fig.set_size_inches(12, 5)
            width = 0.75
            ax.barh(x, y, width, color = "green")    
            container = ax.containers[0]
            ax.bar_label(container, label_type='edge', labels=[f'{x:,.0f}' for x in container.datavalues], padding=4, color='b', fontsize=12)
 
            
            # I could not get this to work - png was entirely facecolor green; disabled facecolor get little squares, jpg blank
            # Tried removing labels, changing format to jpg,... will fix later...as needed... then import slides to Libre Impress
            #
            # plt.savefig("/mnt/USB12TB/testing/Foo_Tiki_Bar_Round3.png",
            #            bbox_inches ="tight",
            #            pad_inches = 1,
            #            transparent = True,
            #            facecolor ="g",
            #            edgecolor ='w',
            #            orientation ='landscape')       
            
            plt.show()
            plt.close()                         
            plotMe = 0       

# End: Program runtime  
end_time = time.perf_counter()
print("Performance Counter", end_time - start_time, "seconds")
end_time = time.time() - begin_time
print("Total execution time: ", format_timespan(end_time))

# HP Omen 2080 32GB Ram laptop /w USB 12TB external disk for both reads/writes
# Performance Counter 3964.969140059009 seconds
# Total execution time:  1 hour, 6 minutes and 4.97 seconds
