from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
from airflow.hooks.postgres_hook import PostgresHook
import json
import numpy as np


def load_data(ds, **kwargs):
        """
         Processes the json data, checks the types and enters into the 
         Postgres database.
        """
    
        pg_hook = PostgresHook(postgres_conn_id='weather_id')

        file_name = str(datetime.now().date()) + '.json'
        tot_name = os.path.join(os.path.dirname(__file__),'data',file_name)

        with open(tot_name, 'r') as inputfile:
              doc = json.load(inputfile)

        city        = str(doc['name'])
        country     = str(doc['sys']['country'])
        lat         = float(doc['coord']['lat'])
        lon         = float(doc['coord']['lon'])
        humid       = float(doc['main']['humidity'])
        press       = float(doc['main']['pressure'])
        min_temp    = float(doc['main']['temp_min']) - 273.15
        max_temp    = float(doc['main']['temp_max']) - 273.15
        temp        = float(doc['main']['temp']) - 273.15
        weather     = str(doc['weather'][0]['description'])
        todays_date = datetime.now().date()

        valid_data = True
        for valid in np.isnan([lat, lon, humid, press, min_temp, max_temp, temp]):
            if valid is False:
                    valid_data = False
                    break

        row = (city, country, lat, lon, todays_date, humid, press, min_temp,
               max_temp, temp, weather)

        insert_cmd = """INSERT INTO weather_table
                    (city, country, latitude, longitude,
                     todays_date, humidity, pressure,
                     min_temp, max_temp, temp, weather)
                     VALUES
                     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        if valid_data is True:
               pg_hook.run(insert_cmd, parameters=row)



default_args = {
            'owner' : 'riyansha',
            'depends_on_past' :False,
            'email' :['navitayash25@gmail.com'],
            'email_on_failure': False,
            'email_on_retry' : False,
            'retries': 5,
            'retry_delay': timedelta(minutes=1)
           }


dag = DAG(
           dag_id='weatherdag',
           default_args=default_args,
           schedule_interval=None,
           start_date=datetime(2021,5,3)
         )


task1 = BashOperator(
    task_id='get_weather',
    bash_command='python3 /c/Users/Admin/airflow/DAGS/src/getweather.py',
    retries=3,
    dag=dag
)

#task2 = BashOperator(
    #task_id='sleep',
    #bash_command='sleep 5',
    #dag=dag
#)
    
task2 = PythonOperator(
    task_id='transform_load',
    provide_context=True,
    python_callable=load_data,
    dag=dag
)

task1 >> task2 

