from flask import Flask, render_template, request
import airflow_client.client
from airflow_client.client.api import config_api, dag_api, dag_run_api
from airflow_client.client.model.dag_run import DAGRun
#from airflow_client.client.api_client import ApiClient, Endpoint as _Endpoint
#from airflow_client.client.model.config import Config
from airflow_client.client.model.error import Error
from flask_sqlalchemy import SQLAlchemy
import psycopg2
from jinja2 import Template
from pprint import pprint
import requests
import json
import time


try:
     conn = psycopg2.connect(database="WeatherDB", user="postgres", password="riyansha", host="localhost", port="5432")
     print("connected")
     
except:
     print("unable to connect")     


def write_json(data, filename='get_city.json'):
    
    with open(filename, "w") as file:
        data = json.dump(data, file, indent=4)

def trigger():
     configuration = airflow_client.client.Configuration(
          host = "http://localhost:8080/api/v1",
          username = 'riyansha',
          password = 'singh@123'
     )

     dag_id = 'weatherdag'

     with airflow_client.client.ApiClient(configuration) as api_client:
          #conf_api_instance = config_api.ConfigApi(api_client) 
          #try:
               #api_response = conf_api_instance.get_config()
               #pprint(api_response)

          #except airflow_client.client.OpenApiException as e:
               #print("Exception when calling ConfigApi->get_config: %s\n" % e)

          dag_run_api_instance = dag_run_api.DAGRunApi(api_client)     
          try:
               dag_run= DAGRun(              
                    #dag_run_id='manual__2021-05-29T17:19:19.211364+00:00',
                    dag_id=dag_id,
                    external_trigger=True,
               )
               api_response = dag_run_api_instance.post_dag_run(dag_id, dag_run)
               pprint(api_response)
          except airflow_client.client.exceptions.OpenApiException as e:
               print("Exception when calling DAGRunAPI->post_dag_run: %s\n" %e)   

app = Flask(__name__)

     
@app.route("/", methods=['POST', 'GET'])
def home():
     return render_template('main.html')

@app.route("/services", methods=['POST', 'GET'])
def index():
     return render_template('find.html')

     
@app.route("/results", methods=['POST', 'GET'])
def render_results():
     place = request.form['location']

     with open('get_city.json') as json_file:
          data = json.load(json_file) 
          temp = data["weather_input"]
          y = {"city": place }
          temp.append(y)

     write_json(data) 

     trigger()

     #time.sleep(40)
     
     cursor = conn.cursor()
     cursor.execute("SELECT * FROM weather_table ORDER BY todays_date DESC LIMIT 1;")
     result = cursor.fetchone()
     print(result)
     time.sleep(10)
     return render_template('show.html', result=result)


@app.route("/contact", methods=['POST', 'GET'])
def contact():
     return render_template('contact.html')
          
@app.route("/gallery", methods=['POST', 'GET'])
def gallery():
     return render_template('gallery.html')
                    
     


if __name__=='__main__':
    app.run(debug=True)         
