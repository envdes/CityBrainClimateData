#!/bin/bash
source /root/miniconda3/bin/activate citybrain_test_env

# example: s3path='s3://ncar-cesm-lens/atm/daily/cesmLE-RCP85-PRECSC.zarr'
s3path=$1
echo "s3path: $s3path"

remaining=${s3path#*//}
vartype=${remaining%%/*}
remaining=${remaining#*/}
component=${remaining%%/*}
remaining=${remaining#*/}
frequency=${remaining%%/*}
tablename=${remaining#*/}
tablename=${tablename%.zarr}
tablename=${tablename//-/_}
featurename=${tablename##*_}

echo "vartype: $vartype"
echo "component: $component"
echo "frequency: $frequency"
echo "tablename: $tablename"
echo "featurename: $featurename"

# export variables to cesm1_variables.json
export vartype=$vartype
export component=$component
export frequency=$frequency
export tablename=$tablename
export featurename=$featurename

JSON_FILE_PATH="/root/airflow/dags/cesm1_variables.json"
touch "$JSON_FILE_PATH"
JSON_STRING='{"s3path": "'"$s3path"'","vartype": "'"$vartype"'", "component": "'"$component"'", "frequency": "'"$frequency"'", "tablename": "'"$tablename"'", "featurename": "'"$featurename"'"}'
echo "$JSON_STRING" > "$JSON_FILE_PATH"

# echo "download from $s3path and save in /datadisk/cesm1_raw/$component/$frequency/$tablename.zarr"

aws s3 sync $s3path /datadisk/cesm1_raw/$component/$frequency/$tablename.zarr --no-sign-request