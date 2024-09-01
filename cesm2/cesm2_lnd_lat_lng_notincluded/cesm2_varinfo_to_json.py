import pandas as pd
import json

def cesm2_varinfo_to_json_workflow(s3path):
    print(f"Received component in cesm2_varinfo_to_json.py: {s3path}")
    
    cesm1dict = pd.read_csv('/root/citybrain_aws-cesm2-le.csv')
    row_with_data = cesm1dict[cesm1dict['path']==s3path]
    row_idx = row_with_data.index.values[0]
    print(f"row_idx:{row_idx}")
    featurename =  row_with_data['variable'].values[0]
    print(f"featurename:{featurename}")
    feature_explain = row_with_data['long_name'].values[0]
    print(f"feature_explain:{feature_explain}")
    component = row_with_data['component'].values[0]
    print(f"component:{component}")
    experiment = row_with_data['experiment'].values[0]
    print(f"experiment:{experiment}")
    forcing_variant = row_with_data['forcing_variant'].values[0]
    print(f"forcing_variant:{forcing_variant}")
    frequency = row_with_data['frequency'].values[0]
    print(f"frequency:{frequency}")
    vertical_levels = row_with_data['vertical_levels'].values[0]
    vertical_levels = str(vertical_levels)
    print(f"vertical_levels: {vertical_levels}")
    spatial_domain = row_with_data['spatial_domain'].values[0]
    print(f"spatial_domain: {spatial_domain}")
    units = row_with_data['units'].values[0]
    print(f"units:{units}")
    start_time = row_with_data['start_time'].values[0]
    print(f"start_time:{start_time}")
    end_time = row_with_data['end_time'].values[0]
    print(f"end_time:{end_time}")
    
    with open('/root/airflow/dags/cesm2_variables.json', 'r') as f:
        variables = json.load(f)
        print(variables)
        
    newinfo = {
        "feature_explain":feature_explain,
        "experiment":experiment,
        "forcing_variant":forcing_variant,
        "vertical_levels":vertical_levels,
        "spatial_domain":spatial_domain,
        "units":units,
        "start_time":start_time,
        "end_time":end_time
    }
    
    variables.update(newinfo)
    
    # Write the updated data back to the JSON file
    with open('/root/airflow/dags/cesm2_variables.json', 'w') as f:
        json.dump(variables, f)  
    
