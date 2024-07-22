import pandas as pd
import json
import pyarrow


# EXTRACT DATA
def extraction(file):
    with open(file) as f:
        data = json.load(f)

    # Extract the features
    features = data['features']

    # Normalize the features into a flat DataFrame
    df = pd.json_normalize(features)

    df.drop(columns=['geometry.coordinates', 'type','geometry.type', 'properties.recordid'],inplace=True)
    
    # Rename columns to remove prefixes
    df.columns = df.columns.str.replace('properties.', '', regex=False)
    df.columns = df.columns.str.replace('geometry.', '', regex=False)
    
    return df

# TRANSFORM DATA
def transform(data: pd.DataFrame) -> pd.DataFrame:
    data['datetime'] = pd.to_datetime(data['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S.%f') 
    data['airtemperature'] = (data['airtemperature'].astype(float) - 32)/1.8  # convert to celsius
    data['roadsurfacetemperature'] = (data['roadsurfacetemperature'].astype(float) - 32)/1.8 # convert to celsius
    
    return data

def load(data: pd.DataFrame):
    data.to_parquet("road_weather.parquet")
    

def execute_etl(file):
    data = extraction(file)
    transformed_data = transform(data)
    load(transformed_data)