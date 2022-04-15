# `CountAndPlotNumberNaNs.py` 
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
CSV_DATA_DIR_IN = '/mnt/USB12TB/WashingtonStateCannabisDatasets/11_2021/Original_Data/datasets'

# set dir where sumNaN csv files are written
CSV_DATA_DIR_OUT = "/mnt/USB12TB/testing"

# set dir to read sumNaN csv files 
sumNaN_CSV_DATA_DIR_IN = '/mnt/USB12TB/testing'

# set dir_out where plot png files go
sumNaN_CSV_DATA_DIR_OUT = "/mnt/USB12TB/testing"

# list of the csv files to sumNaN
csv_files = [ "Areas_0.csv", "LabResults_0.csv", "LabResults_1.csv", "LabResults_2.csv", 
            "Licensees_0.csv", "MmeUser_0.csv", "Strains_0.csv", 
            "Users_0.csv", "Batches_0.csv", "Disposals_0.csv", 
            "Inventories_0.csv", "InventoryAdjustments_0.csv", 
            "InventoryAdjustments_1.csv", "InventoryAdjustments_2.csv", "InventoryTypes_0.csv",  
            "Plants_0.csv", "SaleItems_0.csv", "SaleItems_1.csv", "SaleItems_2.csv",
            "SaleItems_3.csv", "Sales_0.csv", "Sales_1.csv", "Sales_2.csv" ]

# list of the expected sumNaN csv files 
sumNaN_csv_files = [ "sumNaNAreas_0.csv", "sumNaNBatches_0.csv", "sumNaNDisposals_0.csv", 
            "sumNaNInventoryAdjustments_0.csv", "sumNaNInventoryAdjustments_1.csv", 
            "sumNaNInventoryAdjustments_2.csv","sumNaNInventories_0.csv", 
            "sumNaNInventoryTypes_0.csv", "sumNaNLabResults_0.csv", 
            "sumNaNLabResults_1.csv", "sumNaNLabResults_2.csv", 
            "sumNaNLicensees_0.csv", "sumNaNMmeUser_0.csv", "sumNaNPlants_0.csv", 
            "sumNaNStrains_0.csv", "sumNaNSaleItems_0.csv", "sumNaNSaleItems_1.csv", 
            "sumNaNSaleItems_2.csv", "sumNaNSaleItems_3.csv", "sumNaNSales_0.csv", 
            "sumNaNSales_1.csv", "sumNaNSales_2.csv", "sumNaNUsers_0.csv", ]

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
                    iterator=True, sep='\t', encoding='utf-16', index_col=None, header=0 ):
        
        row_count += len(chunk_df)

        chunk_df_series_sum += (chunk_df.isna().sum())

        chunk_df.index += j         
        j = chunk_df.index[-1]+1
        print('| index: {}'.format(j))
        num_rows = print('| index: {}'.format(j))            

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
        
        substring = "Areas"
        if search(substring, fullstring):
            sumNaNAreas_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNAreas_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Areas dataset # of NaNs")
                  
        substring = "Batches"
        if search(substring, fullstring):
            sumNaNBatches_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNBatches_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Batches dataset # of NaNs")

        substring = "Disposals"
        if search(substring, fullstring):
            sumNaNDisposals_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNDisposals_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Disposals dataset # of NaNs")
                    
        substring = "Inventories"
        if search(substring, fullstring):
            sumNaNInventories_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNInventories_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Inventories dataset # of NaNs")
                       
        substring = "InventoryTypes"
        if search(substring, fullstring):
            sumNaNInventoryTypes_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNInventoryTypes_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("InventoryTypes dataset # of NaNs")
  
        substring = "Licensees"
        if search(substring, fullstring):
            sumNaNLicensees_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNLicensees_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Licensees dataset # of NaNs")
            
        substring = "MmeUser"
        if search(substring, fullstring):
            sumNaNMmeUser_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNMmeUser_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("MmeUser dataset # of NaNs")

        substring = "Plants"
        if search(substring, fullstring):
            sumNaNPlants_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNPlants_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Plants dataset # of NaNs")
        
        substring = "Strains"
        if search(substring, fullstring):
            sumNaNStrains_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNStrains_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Strains dataset # of NaNs")
        
        substring = "Users"
        if search(substring, fullstring):
            sumNaNUsers_df = pd.DataFrame(chunk_df)
            SortedAll = sumNaNUsers_df.sort_values(by=[1])
            plotMe = 1
            fig, ax = plt.subplots(constrained_layout=True)
            plt.title("Users dataset # of NaNs")           

        # Lets add up InventoryAdjustments[0-2]
        substring = "InventoryAdjustments"

        if search(substring, fullstring):
            
            if InventoryAdjustment_count == 0:
                sumNaNInventoryAdjustments_0_df_i = pd.DataFrame(chunk_df)
                sumNaNInventoryAdjustments_0_df = (sumNaNInventoryAdjustments_0_df_i.set_index(sumNaNInventoryAdjustments_0_df_i.columns[0]))
                
            if InventoryAdjustment_count == 1:
                sumNaNInventoryAdjustments_1_df_i = pd.DataFrame(chunk_df)
                sumNaNInventoryAdjustments_1_df = (sumNaNInventoryAdjustments_1_df_i.set_index(sumNaNInventoryAdjustments_1_df_i.columns[0]))               
     
                sumNaNInventoryAdjustments_01_df = sumNaNInventoryAdjustments_0_df.add(sumNaNInventoryAdjustments_1_df, fill_value=0)
                
            if InventoryAdjustment_count == 2:
                sumNaNInventoryAdjustments_2_df_i = pd.DataFrame(chunk_df)
                sumNaNInventoryAdjustments_2_df = (sumNaNInventoryAdjustments_2_df_i.set_index(sumNaNInventoryAdjustments_2_df_i.columns[0]))                         
                
                sumNaNInventoryAdjustments_012_df = sumNaNInventoryAdjustments_01_df.add(sumNaNInventoryAdjustments_2_df, fill_value=0)
                
                reIndex = sumNaNInventoryAdjustments_012_df.reset_index(col_level=1)
                                
                sumNaNInventoryAdjustments_df = pd.DataFrame(reIndex)
                SortedAll = sumNaNInventoryAdjustments_df.sort_values(by=[1])
                plotMe = 1
                fig, ax = plt.subplots(constrained_layout=True)
                plt.title("InventoryAdjustments dataset # of NaNs")                
        
            InventoryAdjustment_count += 1 

        # Lets add up LabResults[0-2]

        substring = "LabResults"

        if search(substring, fullstring):
            
            if LabResults_count == 0:
                sumNaNLabResults_0_df_i = pd.DataFrame(chunk_df)
                sumNaNLabResults_0_df = (sumNaNLabResults_0_df_i.set_index(sumNaNLabResults_0_df_i.columns[0]))
                
            if LabResults_count == 1:
                sumNaNLabResults_1_df_i = pd.DataFrame(chunk_df)
                sumNaNLabResults_1_df = (sumNaNLabResults_1_df_i.set_index(sumNaNLabResults_1_df_i.columns[0]))                   
                sumNaNLabResults_01_df = sumNaNLabResults_0_df.add(sumNaNLabResults_1_df, fill_value=0)
                
            if LabResults_count == 2:
                sumNaNLabResults_2_df_i = pd.DataFrame(chunk_df)
                sumNaNLabResults_2_df = (sumNaNLabResults_2_df_i.set_index(sumNaNLabResults_2_df_i.columns[0]))                                         
                sumNaNLabResults_012_df = sumNaNLabResults_01_df.add(sumNaNLabResults_2_df, fill_value=0)               
                reIndex = sumNaNLabResults_012_df.reset_index(col_level=1)                                      
                sumNaNLabResults_df = pd.DataFrame(reIndex)
                SortedAll = sumNaNLabResults_df.sort_values(by=[1])
                plotMe = 1
                fig, ax = plt.subplots(constrained_layout=True)
                plt.title("LabResults dataset # of NaNs")
        
            LabResults_count += 1 
      
        # Lets add up SaleItems[0-3]
        substring = "SaleItems"

        if search(substring, fullstring):
            
            if SaleItems_count == 0:
                sumNaNSaleItems_0_df_i = pd.DataFrame(chunk_df)
                sumNaNSaleItems_0_df = (sumNaNSaleItems_0_df_i.set_index(sumNaNSaleItems_0_df_i.columns[0]))
                
            if SaleItems_count == 1:
                sumNaNSaleItems_1_df_i = pd.DataFrame(chunk_df)
                sumNaNSaleItems_1_df = (sumNaNSaleItems_1_df_i.set_index(sumNaNSaleItems_1_df_i.columns[0]))                  
                sumNaNSaleItems_01_df = sumNaNSaleItems_0_df.add(sumNaNSaleItems_1_df, fill_value=0)
                
            if SaleItems_count == 2:
                sumNaNSaleItems_2_df_i = pd.DataFrame(chunk_df)
                sumNaNSaleItems_2_df = (sumNaNSaleItems_2_df_i.set_index(sumNaNSaleItems_2_df_i.columns[0]))                                        
                sumNaNSaleItems_012_df = sumNaNSaleItems_01_df.add(sumNaNSaleItems_2_df, fill_value=0)
                
            if SaleItems_count == 3:
                sumNaNSaleItems_3_df_i = pd.DataFrame(chunk_df)
                sumNaNSaleItems_3_df = (sumNaNSaleItems_3_df_i.set_index(sumNaNSaleItems_3_df_i.columns[0]))                                        
                sumNaNSaleItems_0123_df = sumNaNSaleItems_012_df.add(sumNaNSaleItems_3_df, fill_value=0)               
                reIndex = sumNaNSaleItems_0123_df.reset_index(col_level=1)                         
                sumNaNSaleItems_df = pd.DataFrame(reIndex)               
                SortedAll = sumNaNSaleItems_df.sort_values(by=[1])
                plotMe = 1
                fig, ax = plt.subplots(constrained_layout=True)
                plt.title("SaleItems dataset # of NaNs")
        
            SaleItems_count += 1 
        
        # Lets add up Sales[0-2]
        substring = "Sales_"

        if search(substring, fullstring):
            
            if Sales_count == 0:
                sumNaNSales_0_df_i = pd.DataFrame(chunk_df)
                sumNaNSales_0_df = (sumNaNSales_0_df_i.set_index(sumNaNSales_0_df_i.columns[0]))
                
            if Sales_count == 1:
                sumNaNSales_1_df_i = pd.DataFrame(chunk_df)
                sumNaNSales_1_df = (sumNaNSales_1_df_i.set_index(sumNaNSales_1_df_i.columns[0]))               
     
                sumNaNSales_01_df = sumNaNSales_0_df.add(sumNaNSales_1_df, fill_value=0)
                
            if Sales_count == 2:
                sumNaNSales_2_df_i = pd.DataFrame(chunk_df)
                sumNaNSales_2_df = (sumNaNSales_2_df_i.set_index(sumNaNSales_2_df_i.columns[0]))                                        
                sumNaNSales_012_df = sumNaNSales_01_df.add(sumNaNSales_2_df, fill_value=0)               
                reIndex = sumNaNSales_012_df.reset_index(col_level=1)                        
                sumNaNSales_df = pd.DataFrame(reIndex)
                SortedAll = sumNaNSales_df.sort_values(by=[1])
                plotMe = 1
                fig, ax = plt.subplots(constrained_layout=True)
                plt.title("Sales dataset # of NaNs")

            Sales_count += 1 
            
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
