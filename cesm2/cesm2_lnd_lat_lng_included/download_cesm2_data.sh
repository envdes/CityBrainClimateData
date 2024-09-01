#!/bin/bash
source /root/miniconda3/bin/activate cesm2

s3path=$1
echo "s3path: $s3path"

remaining=${s3path#*//}
echo "remaining: $remaining"
vartype=${remaining%%/*}
remaining=${remaining#*/}
component=${remaining%%/*}
remaining=${remaining#*/}
frequency=${remaining%%/*}
tablename=${remaining#*/}
tablename=${tablename%.zarr}
tablename=${tablename//-/_}

IFS='_' read -ra parts <<< "$tablename"
cesmnum="${parts[0]}"
experiment="${parts[1]}"
forcing_variant="${parts[2]}"
featurename="${parts[3]}"

echo "vartype: $vartype"
echo "component: $component"
echo "frequency: $frequency"
echo "tablename: $tablename"
echo "experiment: $experiment"
echo "forcing_variant: $forcing_variant"
echo "featurename: $featurename"

export vartype=$vartype
export component=$component
export frequency=$frequency
export tablename=$tablename
export experiment=$experiment
export forcing_variant=$forcing_variant
export featurename=$featurename

JSON_FILE_PATH="/root/airflow/dags/cesm2_variables.json"
touch "$JSON_FILE_PATH"
JSON_STRING='{"s3path": "'"$s3path"'","vartype": "'"$vartype"'", "component": "'"$component"'", "frequency": "'"$frequency"'", "tablename": "'"$tablename"'","experiment": "'"$experiment"'", "forcing_variant": "'"$forcing_variant"'", "featurename": "'"$featurename"'"}'
echo "$JSON_STRING" > "$JSON_FILE_PATH"

aws s3 sync $s3path /datadisk/cesm2_raw/$forcing_variant/$component/$frequency/$tablename.zarr --no-sign-request
