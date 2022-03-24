# `QuantityColumnDerivedfromSaleItemsNames.py`
# Author: Candace O'Sullivan-Sutherland 3/23/2022
#
# Wrapper to run `quantity_from_description.ipyn` on all Washington State dataset csv files.
# SaleItems_0.csv (43.7GB)
# SaleItems_1.csv (49.3GB)
# SaleItems_2.csv (49.6GB)
# SaleItems_3.csv (42.9GB)
#
#    `quantity_from_description.ipyn`
#    Author: UFO Software, LLC Created: Sunday, March 7, 2022 22:36
#    License GPLv3+: GNU GPL version 3 or later https://gnu.org/licenses/gpl.html 
#    This is free software: you are free to change and redistribute it. 
#    There is NO WARRANTY, to the extent permitted by law.
#    Reads in a subset of the SalesItems_3.csv, extracts product quantity information from 
#    the description column and stores in the quantity column.

# Import Modules
import os
import gc
import time
import pandas as pd
import numpy as np
import spacy
from spacy.pipeline import EntityRuler
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.options.display.float_format = "{:.2f}".format

# Begin: Program runtime
start_time = time.perf_counter()
from humanfriendly import format_timespan
begin_time = time.time()

# set directory where original dataset csv files are read from
CSV_DATA_DIR_IN = "/mnt/HDD"

# set directory where dataset massaged csv files are written to
# CSV_DATA_DIR_OUT = "/mnt/USB12TB/WashingtonStateNewCSVs"

# specify data types to reduce memory footprint
col_dtypes = {'global_id': 'string',
              'inventory_id':'string',
              'sale_id':'string',
              'batch_id':'string',
              'description':'string',
              'qty':'float16',
              'uom':'category',
              'unit_price':'float16',
              'price_total':'float16',
              'name':'string'
             }
    

date_cols = ['created_at',
             'updated_at',
             'sold_at'
            ]

# create a blank English pipeline
nlp = spacy.blank("en")
        
# create an entity ruler to label units preceded by a number as a quantity
ruler = nlp.add_pipe("entity_ruler")
weights_pattern = [
    {"LIKE_NUM": True},
    {"LOWER": {"IN": ["g", "kg", "grams", 'gram', "mg", "kilograms", "kilogram", "milligrams", "milligram", 'ounce', 'ounces', 'oz', 'lb']}}
]
patterns = [{"label": "QUANTITY", "pattern": weights_pattern}]
ruler.add_patterns(patterns)
        
def get_quantity(x):

    if str(x)!='nan' and type(x)=='str':  
      doc = nlp(x)
      quantity_entities = [ent for ent in doc.ents if (ent.label_ == "QUANTITY") ]
      if len(quantity_entities) == 0:
        # if no quantity is found return 0
        return 0
      else:
        # return the last quantity found since it is usually the one we are looking for
        ent = list(quantity_entities)[-1]
        return ent.text


# Read Large csv Files in chunks
print("BEGIN: .......................................")

SaleItems_Files = [ "SaleItems_0.csv", "SaleItems_1.csv", "SaleItems_2.csv", "SaleItems_3.csv" ]
                        
for file_name in SaleItems_Files:
    
    print(f'Working with filename:', file_name)
    
    DATA_FILE_IN  = f'{CSV_DATA_DIR_IN}/{file_name}' 
    DATA_FILE_OUT = f'{CSV_DATA_DIR_OUT}/{file_name}' 
    
    chunksize = 1000000
    i = 0
    j = 0
    print('| index: {}'.format(j))
        
    for chunk_df in pd.read_csv(f'{DATA_FILE_IN}', chunksize=chunksize, error_bad_lines=False, 
                    iterator=True, sep='\t', encoding='utf-16', index_col=None, header=0 ):
        
        # Before we copy chunk_df.name NaN to chunk.description below - replace NaN with "nd" (no data)
        chunk_df.name = np.where(chunk_df.name.isna(), "nd", chunk_df.name )

        # Detect NaN values
        chunk_df.description = np.where(chunk_df.description.isna(), chunk_df.name, chunk_df.description)
              
        # apply nlp to each row and fill in the quantity column
        chunk_df['quantity'] = chunk_df.description.apply(lambda x: get_quantity(x))
        
        # Write with headers to new csv or append to existing csv without headers
        hdr = False  if os.path.isfile(f'{DATA_FILE_OUT}') else True
        file_exists = True if os.path.isfile(f'{DATA_FILE_OUT}') else False
        chunk_df.to_csv(f'{DATA_FILE_OUT}', sep='\t', encoding='utf-16', header = hdr, mode='a' if file_exists else 'w')

        chunk_df.index += j         
        j = chunk_df.index[-1]+1
        print('| index: {}'.format(j))


print("END:   .......................................")
  
# End: Program runtime  
end_time = time.perf_counter()
print("Performance Counter", end_time - start_time, "seconds")
end_time = time.time() - begin_time
print("Total execution time: ", format_timespan(end_time))


# NOTE: Total Execution Time: ~7 hours, 58 minutes and 29.66 seconds

