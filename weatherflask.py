from flask import Flask, render_template, request
import airflow_client.client
from airflow_client.client.api import config_api, dag_api, dag_run_api
from airflow_client.client.model.dag_run import DAGRun
#from airflow_client.client.api_client import ApiClient, Endpoint as _Endpoint
#from airflow_client.client.model.config import Config
from airflow_client.client.model.error import Error
import psycopg2
from pprint import pprint
import requests

def connect_database():
     try:
          conn = psycopg2.connect(database="WeatherDB", user="postgres", password="riyansha", host="localhost", port="5432")
          print("connected")
     except:
          print("unable to connect")     
     cursor= conn.cursor

app = Flask(__name__)

#conn = self.connection_from_host("http://localhost:5000/")
#r = conn.request('GET','/')

configuration = airflow_client.client.Configuration(
     host = "http://localhost:8080/api/v1",
     username = 'riyansha',
     password = 'singh@123'
)


#conn = self.connection_from_host("http://localhost/")
conn= psycopg2.connect(database="WeatherDB", user="postgres", password="riyansha", host="localhost", port="5432")
cursor= conn.cursor

@app.route("/", methods=['POST', 'GET'])
def index():
     #if request.method == 'POST':
          #city = request.form['location']
     return render_template('index.html')
     
     

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
#c= airflow_client
#c.DAGRun(dag_id='weatherdag', conf={})

#thread = airflow_client
#thread.('weatherdag')

#class Client(ApiClient.Client):
     #def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
          #dag_run = trigger_dag.trigger_dag(dag_id='weatherdag', run_id='scheduled__2021-05-05T09:17:00+00:00', conf={}, execution_date='2021-05-05, 14:47:00')
          #return f"{dag_run}"


@app.route('/results')
def results():
     cursor.execute("SELECT * FROM public.weather_table ORDER BY todays_date LIMIT 1")
     result = cursor.fetchall()
     return render_template('results.html', data=result)

if __name__=='__main__':
    app.run(debug=True)         
