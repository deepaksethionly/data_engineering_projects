#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 24 22:52:52 2023

@author: deepaksethi
"""

@transformer

def transform(data):

    data → load_cab_share_data
    
    import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(cab_project, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    cab_project['pep_pickup_datetime']=pd.to_datetime(cab_project['tpep_pickup_datetime'])
    cab_project['tpep_dropoff_datetime']=pd.to_datetime(cab_project['tpep_dropoff_datetime'])

    cab_project=cab_project.drop_duplicates().reset_index(drop=True)
    cab_project['trip_id']=cab_project.index

    data_time_columns=['tpep_pickup_datetime','tpep_dropoff_datetime']

    df_datetime=pd.DataFrame()
    for date_time_column in cab_project[data_time_columns]:
        df_datetime[date_time_column]=pd.to_datetime(cab_project[date_time_column])
    df_datetime=pd.DataFrame(df_datetime,columns=data_time_columns)


    datetime_deviated_columns_list = ['day', 'month', 'year', 'hour', 'weekday']  
    dervived_columns=['pickup','dropoff']

    def datetime_function(df, deviated_list, data_time_columns,dervived_columns):
        for date_column in data_time_columns:
            for col_type in dervived_columns:
                for deviation in deviated_list:
                    new_column_name = col_type + '_' + deviation
                    if deviation == 'day':
                        df[new_column_name] = df[date_column].dt.day
                    elif deviation == 'month':
                        df[new_column_name] = df[date_column].dt.month
                    elif deviation == 'year':
                        df[new_column_name] = df[date_column].dt.year
                    elif deviation == 'hour':
                        df[new_column_name] = df[date_column].dt.hour
                    elif deviation == 'weekday':
                        df[new_column_name] = df[date_column].dt.weekday
                        
        pickup_columns =[col_name for col_name in df.columns if "pickup" in col_name]
        
        dropoff_columns =[col_name for col_name in df.columns if "dropoff" in col_name]
        
        pickup_df=df[pickup_columns]
        
        dropoff_df=df[dropoff_columns]

        return pickup_df,dropoff_df

    df_datetime_pickup,df_datetime_dropoff=datetime_function(df_datetime, datetime_deviated_columns_list, data_time_columns,dervived_columns)

    df_datetime_pickup=df_datetime_pickup.reset_index(drop=True)
    df_datetime_pickup['datetime_id_pickup']=df_datetime_pickup.index

    df_datetime_dropoff=df_datetime_dropoff.reset_index(drop=True)
    df_datetime_dropoff['datetime_id_dropoff']=df_datetime_dropoff.index

    df_pickup_dim=cab_project[['pickup_longitude','pickup_latitude']]
    df_pickup_dim=df_pickup_dim.reset_index(drop=True)
    df_pickup_dim['pickup_location_id']=df_pickup_dim.index

    df_dropoff_dim=cab_project[['dropoff_longitude','dropoff_latitude']]
    df_dropoff_dim=df_dropoff_dim.reset_index(drop=True)
    df_dropoff_dim['dropoff_location_id']=df_dropoff_dim.index

    df_ratecode_dim=cab_project[['RatecodeID']].copy()
    df_ratecode_dim['rate_code_id']=df_ratecode_dim.index

    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }

    df_ratecode_dim['ratecode_name']=df_ratecode_dim['RatecodeID'].map(rate_code_type)

    df_store_fwd=cab_project[['store_and_fwd_flag']].copy()
    df_store_fwd['store_fw_id']=df_store_fwd.index

    df_store_fwd['store_and_fwd_name']=df_store_fwd['store_and_fwd_flag'].map({'N':1,'Y':0})

    df_payment_dim=cab_project[['payment_type']].copy()
    df_payment_dim['payment_type_id']=df_payment_dim.index

    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }

    df_payment_dim['payment_type_name']=df_payment_dim['payment_type'].map(payment_type_name)

    share_cab_fact_table=cab_project.merge(df_datetime_pickup,left_on='trip_id',right_on='datetime_id_pickup') \
                    .merge(df_datetime_dropoff,left_on='trip_id',right_on='datetime_id_dropoff') \
                    .merge(df_pickup_dim,left_on='trip_id',right_on='pickup_location_id') \
                    .merge(df_dropoff_dim,left_on='trip_id',right_on='dropoff_location_id') \
                    .merge(df_ratecode_dim,left_on='trip_id',right_on='rate_code_id') \
                    .merge(df_store_fwd,left_on='trip_id',right_on='store_fw_id') \
                    .merge(df_payment_dim,left_on='trip_id',right_on='payment_type_id') \
                    [['VendorID','trip_id','passenger_count','trip_distance','datetime_id_pickup',
                      'datetime_id_dropoff','pickup_location_id','dropoff_location_id',
                      'rate_code_id','store_fw_id','payment_type_id','fare_amount',
                      'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge',
                      'total_amount']]

    return{"df_datetime_pickup":df_datetime_pickup.to_dict(orient='dict'),
            "df_datetime_dropoff":df_datetime_dropoff.to_dict(orient='dict'),
            "df_pickup_dim":df_pickup_dim.to_dict(orient='dict'),
            "df_dropoff_dim":df_dropoff_dim.to_dict(orient='dict'),
            "df_ratecode_dim":df_ratecode_dim.to_dict(orient='dict'),
            "df_store_fwd":df_store_fwd.to_dict(orient='dict'),
            "df_payment_dim":df_payment_dim.to_dict(orient='dict'),
            "share_cab_fact_table":share_cab_fact_table.to_dict(orient='dict')}
        
            
    
 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
