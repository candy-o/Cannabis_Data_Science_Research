"""
CannabinoidTerpeneDectionPotency1207022.py
Copyright (c) 2022 CSWCFA

Authors:
    Candace O'Sullivan-Sutherland <https://github.com/candy-o>

Created: 12/7/2022
Updated: 12/8/2022
License: <https://github.com/cannlytics/cannlytics/blob/main/LICENSE>

Description: Cannabis Data Science Meetup Dec 7, 2022 notes: USA MA Lab kindly offered 
             "Cannabinoid/Terpene Detection Rates and Avg. Detected Potency Percentages" 
             via png "CannabinoidTerpeneDectionPotency12072-22.py` zips list data into 
             dataframe, sorts then plots.

"""

import pandas as pd
import matplotlib.pyplot as plt

# Cannabinoid Data received Lists
cannabinoids = [ 'd9THC', 'CBD', 'CBN', 'THCa', 'CBDa', 'd8THC', 'CBGa', 'THCV', 'CBDV', 'CBC', 'CBCV', 'CBT', 'CBDVa', 'CBG', 'THCVa', 'CBNa', 'CBL', 'CBCa', 'CBLa' ]
cannabinoids_detection_rate_percentages = [ 98.4, 2.1, 3.9, 100, 27.9, 0.6, 98.4, 1, 0.4, 3.4, 0.3, 8.3, 1.4, 80.6, 68.9, 7, 6.5, 96.3, 9.4 ]
cannabinoids_average_detected_potency_percentages = [ 0.65, 0.46, 0.11, 18.83, 0.82, 0.19, 0.67, 0.1, 0.08, 0.09, 0.06, 0.13, 0.19, 0.11, 0.23, 0.11, 0.1, 0.25, 0.28 ]

# Terpene Data received Lists 
terpenes = [ 'aPinene', 'camphene', 'bMyrcene', 'bPinene', 'd3Carene', 'aTerpinene', 'ocimene', 'dLimonene', 'pCymene', 
             'bOcimene', 'eucalyptol', 'yTerpinene', 'terpinolene', 'linalool', 'isopulegol', 'geraniol', 'bCaryphyllene', 
             'aHumulene', 'nerolidol1', 'nerolidol2', 'guaiol', 'caryophelleneOxide', 'aBisabolol' ]
terpenes_detection_rate_percentages = [ 98.7, 87.7, 99.7, 98.2, 52.5, 50.3, 28.7, 99.3, 58.6, 70.4, 43.2, 55.7, 81.1, 96.6, 36.8, 33, 99.8, 99.7, 83.8, 95.9, 45.9, 56.8, 87.7 ]
terpenes_average_detected_potency_percentages =  [ 0.07, 0.02, 0.22, 0.06, 0.02, 0.02, 0.05, 0.18, 0.02, 0.05, 0.03, 0.02, 0.08, 0.08, 0.07, 0.02, 0.17, 0.06, 0.02, 0.04, 0.02, 0.02, 0.02 ]

# Zip List data into Tuples
list_of_cannabinoids_tuples = list(zip(cannabinoids, cannabinoids_detection_rate_percentages, cannabinoids_average_detected_potency_percentages))
list_of_terpenes_tuples = list(zip(terpenes, terpenes_detection_rate_percentages, terpenes_average_detected_potency_percentages)) 

# Create Dataframes from Tuples
cannabinoids_df = pd.DataFrame(list_of_cannabinoids_tuples, columns=['Cannabinoids', 'Detection Rate(%)', 'Avg. Detected Potency(%)'])
terpenes_df = pd.DataFrame(list_of_terpenes_tuples, columns=['Terpenes', 'Detection Rate(%)', 'Avg. Detected Potency(%)'])

# Print Dataframes
print(cannabinoids_df)
print(terpenes_df)

# Sort and Plot 
cannabinoids_df.sort_values(by='Detection Rate(%)').plot(x='Cannabinoids', y='Detection Rate(%)', kind='barh', title="12/7/2022 USA MA Lab Cannabinoid Detection Rate(%)").legend(loc='lower right')
cannabinoids_df.sort_values(by='Detection Rate(%)').drop([3,4,6,0,17,14,13]).plot(x='Cannabinoids', y='Detection Rate(%)', kind='barh', title="12/7/2022 MA USA Lab Cannabinoid Detection Rate(%) [removed > 20%]").legend(loc='lower right')


cannabinoids_df.sort_values(by='Avg. Detected Potency(%)').plot(x='Cannabinoids', y='Avg. Detected Potency(%)', kind='barh', title="12/7/2022 USA MA Lab Cannabinoid Avg. Detected Potency(%)").legend(loc='lower right')
cannabinoids_df.sort_values(by='Avg. Detected Potency(%)').drop([3]).plot(x='Cannabinoids', y='Avg. Detected Potency(%)', kind='barh', title="12/7/2022 USA MA Lab Cannabinoid Avg. Detected Potency(%) [removed THCa]").legend(loc='lower right')


terpenes_df.sort_values(by='Detection Rate(%)').plot(x='Terpenes', y='Detection Rate(%)', kind='barh', title=("12/7/2022 USA MA Lab Terpenes Detection Rate(%)")).legend(loc='lower right')
terpenes_df.sort_values(by='Avg. Detected Potency(%)').plot(x='Terpenes', y='Avg. Detected Potency(%)', kind='barh', title="12/7/2022 USA MA Lab Terpenes Avg. Detected Potency(%)").legend(loc='lower right')
