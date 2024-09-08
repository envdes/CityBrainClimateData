# CityBrainClimateData

**Introduction**

This project develops data pipelines to streamline the process of extracting CESM data from AWS S3, transforming and loading data into in Citybrain.

The data pipeline includes the following steps:
1. Download data from AWS: Use a bash script to download data from the specified S3 URI.
2. Save parameters in JSON: Save relevant parameters in a JSON file.
3. Data transformation: Convert the downloaded data from Zarr to Parquet and perform data transformations.
4. Create table in Citybrian: Create a table in Citybrian using the Parquet files and the parameters in the JSON file.
5. Quality Assurance (QA): Download sample data from Citybrian to check if the data pipeline has functioned correctly and assess data quality.   

For example, CESM1 data pipeline:
![plot](./cesm1/Citybrain_CESM1_data_pipeline.png)

**How to run the data pipeline**

In this project, Apache Airflow DAG is used to orchestrate the workflow. 

Before running a data pipeline: 
- Citybrain platform access: Sign up for a Citybrain platform account 
- Set up the environment and install required libraries
    - [Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
- Donwload **aws-cesm1-le.csv** and **aws-cesm2-le.csv**
    - aws-cesm1-le.csv can be downloaded from https://ncar.github.io/cesm-lens-aws/ (Data Catalog)
    - aws-cesm2-le.csv can be downloaded from https://ncar-cesm2-lens.s3-us-west-2.amazonaws.com/catalogs/aws-cesm2-le.csv

To run a data pipeline (DAG):
1. In the DAG file, replace the default S3 URI with the S3 URI of the target data.
    For example, in [cesm1/cesm1-dag.py](./cesm1/cesm1_dag.py), change 's3://ncar-cesm-lens/atm/daily/cesmLE-RCP85-QBOT.zarr' to 's3://ncar-cesm-lens/atm/daily/cesmLE-20C-FLNS.zarr': 
    
    **DAG files (xx_dag_xx.py)**:
    - CESM1:
        - [CESM1 DAG (cesm1_dag.py)](./cesm1/cesm1_dag.py) 
    - CESM2:
        - latitude and longitiude included in the data: [CESM2 DAG (cesm2_lnd_dag_lat_lng_included.py)](./cesm2/cesm2_lnd_lat_lng_included/cesm2_lnd_dag_lat_lng_included.py)
        - latitude and longitiude not included in the data: [CESM2 DAG (cesm2_lnd_dag_lat_lng_notincluded.py)](./cesm2/cesm2_lnd_lat_lng_notincluded/cesm2_lnd_dag_lat_lng_notincluded.py)

2. Trigger the DAG execution from Airflow UI or from command line. For example, to trigger [CESM1 DAG (cesm1_dag.py)](./cesm1/cesm1_dag.py): ```airflow dags trigger cesm1_dag```. The DAG will execute the tasks according to the defined task dependencies.  
3. Then monitor the progress and status of each task from Airflow UI or from command line. 

