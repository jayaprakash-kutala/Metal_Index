# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 00:46:53 2018

@author: jayap
"""

# import libraries
import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
pd.options.mode.chained_assignment = None  # default='warn'


def scrap_index(timedelta,method):

    # Calculate date_now & before three_months date    
    date_now = date.today() + relativedelta(days=-timedelta)
    three_months = date_now + relativedelta(months=-3)
    date_now = date_now.strftime('%Y-%m-%d')
    three_months = three_months.strftime('%Y-%m-%d')
    print(date_now)
    
    # extract data from .csv file
    raw_data = pd.read_csv('metal_history_20181008.csv')
    
    # Mapping location with regions
    mapping_dictionary = {'AL':'EAST COAST','CT':'EAST COAST','DE':'EAST COAST','FL':'EAST COAST','GA':'EAST COAST',
                          'IN':'EAST COAST','KY':'EAST COAST','ME':'EAST COAST','MD':'EAST COAST','MA':'EAST COAST',
                          'MI':'EAST COAST','MS':'EAST COAST','NH':'EAST COAST','NJ':'EAST COAST','NY':'EAST COAST',
                          'NC':'EAST COAST','OH':'EAST COAST','PA':'EAST COAST','RI':'EAST COAST','SC':'EAST COAST',
                          'TN':'EAST COAST','VT':'EAST COAST','VA':'EAST COAST','WV':'EAST COAST','AR':'MID WEST',
                          'IL':'MID WEST','IA':'MID WEST','KS':'MID WEST','LA':'MID WEST','MN':'MID WEST','MO':'MID WEST',
                          'NE':'MID WEST','ND':'MID WEST','OK':'MID WEST','SD':'MID WEST','TX':'MID WEST','WI':'MID WEST',
                          'AK':'WEST COAST','AZ':'WEST COAST','CA':'WEST COAST','CO':'WEST COAST','HI':'WEST COAST',
                          'ID':'WEST COAST','MT':'WEST COAST','NV':'WEST COAST','NM':'WEST COAST','OR':'WEST COAST',
                          'UT':'WEST COAST','WA':'WEST COAST','WY':'WEST COAST'}
    raw_data['region'] = raw_data['state'].apply(lambda x: mapping_dictionary[x])
    
    # convert unit TON to CWT of ferrous[unit standardization]
    raw_data.price = raw_data.price.mask(raw_data.unit == 'TON',raw_data.price*0.056)
    raw_data.unit = raw_data.unit.mask(raw_data.unit == 'TON','CWT')
    
    # convert units: LB to CWT of ferrous[unit standardization]
    raw_data.loc[(raw_data['commodity'] == 'Scrap Steel') & (raw_data['unit'] == 'LB'),'price'] = raw_data['price']*112
    raw_data.loc[(raw_data['commodity'] == 'Scrap Steel') & (raw_data['unit'] == 'LB'),'unit'] = 'CWT'
    
    # convert units: CWT to LB of Stainless Steel[unit standardization]
    raw_data.loc[((raw_data['metalType'] == 'Non-Ferrous') & (raw_data['unit'] == 'CWT')) 
                | ((raw_data['commodity'] == 'Scrap Stainless Steel') & (raw_data['unit'] == 'CWT')),
                'price'] = raw_data['price']*0.008929

    raw_data.loc[((raw_data['metalType'] == 'Non-Ferrous') & (raw_data['unit'] == 'CWT'))
                | ((raw_data['commodity'] == 'Scrap Stainless Steel') & (raw_data['unit'] == 'CWT')),'unit'] = 'LB'
    
    # Delete the data older than three months
    raw_data['date'] = pd.to_datetime(raw_data['date']).dt.strftime('%Y-%m-%d')
    raw_data = raw_data[(raw_data['date'] >= three_months) & (raw_data['date'] <= date_now)]
    
    # check the data for standard units
    standard_units = ['LB','CWT','TON']
    raw_data = raw_data[raw_data.unit.isin(standard_units)]
    
    data_avail_dates = raw_data['date'].unique()
    
    # Average number of companies for each product on any day, in the past three months
    locations_data = raw_data.groupby(['productId'])
    average_locations_count = pd.DataFrame(columns = ['ProductId','Product','Commodity','Mean'])
    iterative_index = 0
    for productId,productId_group in locations_data:
        average_product_count = (productId_group['product'].count()/len(data_avail_dates))
        commodity_name = productId_group['commodity'].iloc[0]
        product_name = productId_group['product'].iloc[0]
        data = [productId,product_name,commodity_name,average_product_count]
        average_locations_count.loc[iterative_index] = data
        iterative_index +=1
        
    # Filter metals based on threshold value
    average_locations_count = average_locations_count[(average_locations_count['Mean'] > 10)]
    filtered_metals = average_locations_count['ProductId']
    raw_data = raw_data[raw_data.productId.isin(filtered_metals)]
    
    # Count the frequency of change in the prices for past three months
    frequency_change_data = raw_data.groupby(["location","productId"])
    price_change_date_bycompany = pd.DataFrame(columns=['Location','date'])
    iterative_index = 0
    for loc_proId,loc_prodId_group in frequency_change_data:
        price_difference =  loc_prodId_group['price'].diff().fillna(0)
        price_change_dates = loc_prodId_group[(price_difference != 0)][['date']]
        price_change_dates = pd.to_datetime(price_change_dates['date']).dt.strftime('%Y-%m-%d')
        if(price_change_dates.empty == False):
            last_price_change_date = price_change_dates.iloc[-1]
        else:
            last_price_change_date = three_months
        data = [loc_proId[0],last_price_change_date]
        price_change_date_bycompany.loc[iterative_index] = data
        iterative_index +=1
    latest_price_change = price_change_date_bycompany.groupby('Location')
    for location,location_group in latest_price_change:
        latest_price_change = location_group['date'].max()
        raw_data.loc[(raw_data['location'] == location),'latest_price_change'] = latest_price_change
        
        
    # calculate weights using methods    
    raw_data['date_now'] = date_now
    raw_data['latest_price_change'] = pd.to_datetime(raw_data['latest_price_change'])
    raw_data['date_now'] = pd.to_datetime(raw_data['date_now'])
    raw_data['duration'] = (raw_data['date_now'] - raw_data['latest_price_change']).dt.days
    raw_data['weights'] = ((1/raw_data['duration'])**(1/method)).round(4)
    raw_data['price-weights'] = (raw_data['price']*raw_data['weights'])
    
    # National Price Index
    national_index_data = raw_data.groupby('productId')
    national_index = pd.DataFrame(columns = ['ProductId','Product','Commodity','Region',
                                             'Low','Mean','High','Price Unit'])
    iterative_index = 0
    for productId,productId_group in national_index_data:
        price_quantile_10 = productId_group['price'].quantile(0.1)
        price_quantile_90 = productId_group['price'].quantile(0.9)
        productId_group = productId_group[(productId_group['price'] > price_quantile_10) 
                                      & (productId_group['price'] < price_quantile_90)]
        mean = productId_group['price-weights'].mean()
        low  = productId_group['price-weights'].min()
        high = productId_group['price-weights'].max()
        product_name = productId_group['product'].iloc[0]
        commodity_name = productId_group['commodity'].iloc[0]
        unit_name = productId_group['unit'].iloc[0]
        region = 'National'
        data = [productId,product_name,commodity_name,
                region,low,mean,high,unit_name]
        national_index.loc[iterative_index] = data
        iterative_index +=1  
# national_index = national_index.round({'Low':2,'Mean':2,'High':2})
# national_index
    
    
    # Regional Price Index
    regional_index_data = raw_data.groupby(['productId','region'])
    regional_index = pd.DataFrame(columns = ['ProductId','Product','Commodity','Region',
                                             'Low','Mean','High','Price Unit'])
    iterative_index = 0
    for Id_region,Id_region_group in regional_index_data:
        price_quantile_10 = Id_region_group['price'].quantile(0.1)
        price_quantile_90 = Id_region_group['price'].quantile(0.9)
        Id_region_group = Id_region_group[(Id_region_group['price'] > price_quantile_10) 
                                          & (Id_region_group['price'] < price_quantile_90)]
        if(Id_region_group.empty == False):
            mean = Id_region_group['price-weights'].mean()
            low  = Id_region_group['price-weights'].min()
            high = Id_region_group['price-weights'].max()
            product_name = Id_region_group['product'].iloc[0]
            commodity_name = Id_region_group['commodity'].iloc[0]
            unit_name = Id_region_group['unit'].iloc[0]
            region = Id_region[1]
            data = [Id_region[0],product_name,commodity_name,
                    region,low,mean,high,unit_name]
            regional_index.loc[iterative_index] = data
            iterative_index +=1
    regional_index = regional_index.sort_values(['Region','ProductId'])
    scrap_metal_index = national_index.append(regional_index)
    scrap_metal_index = scrap_metal_index.round({'Low':2,'Mean':2,'High':2})
    
    # Export to excel
    file_name = "Metal_Index-" + 'Method_'+ str(method) +'-' + str(date_now)+".csv"
    scrap_metal_index.to_csv(file_name, index = False)
    print(scrap_metal_index)
    return 0


# Test Report
timedelta = 23

while(timedelta < 55):
    temp1 = scrap_index(timedelta,method=1)
    temp2 = scrap_index(timedelta,method=7)
    temp3 = scrap_index(timedelta,method=13)
    temp4 = scrap_index(timedelta,method=15)
    timedelta += 1
    
    
    
    
    
    
    
    
    
    
    
    