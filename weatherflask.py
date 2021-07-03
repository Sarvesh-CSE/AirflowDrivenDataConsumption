from flask import Flask, render_template, request, redirect, url_for, session
import airflow_client.client
from airflow_client.client.api import config_api, dag_api, dag_run_api
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.error import Error
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import psycopg2
from jinja2 import Template
from pprint import pprint
import requests
import json
import time
import os



try:
     conn = psycopg2.connect(database="WeatherDB", user="postgres", password="riyansha", host="localhost", port="5432")
     print("connected")
     
except:
     print("unable to connect")    

try:
     con = psycopg2.connect(database="Users_info", user="postgres", password="riyansha", host="localhost", port="5432")
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
app.secret_key=os.urandom(24)

@app.route("/", methods=['POST', 'GET'])
def home():
     cursor = conn.cursor()
     cursor.execute("SELECT DISTINCT ON (todays_date) * FROM public.weather_table ORDER BY todays_date desc, date_time desc NULLS LAST, city limit 1;")
     result = cursor.fetchall()
     print(result)
     return render_template('home.html', result = result)
     
@app.route("/results", methods=['POST', 'GET'])
def render_results():
     country = request.form['country']
     place = request.form['location']


     with open('get_city.json') as json_file:
          data = json.load(json_file) 
          temp = data["weather_input"]
          y = {"city": place }
          temp.append(y)

     write_json(data) 

     trigger()

     time.sleep(40)
     
     cursor = conn.cursor()
     cursor.execute("SELECT DISTINCT ON (todays_date) * FROM public.weather_table ORDER BY todays_date desc, date_time desc NULLS LAST, city limit 1;")
     result = cursor.fetchall()

     print(result)
     time.sleep(10)
     return redirect(url_for('home', result = result, place = place, country = country))
     

@app.route("/signup", methods=['POST', 'GET'])
def signup():
     return render_template('signup.html')

@app.route("/success", methods=['POST', 'GET'])
def contact():
     username = request.form['user']
     email = request.form['email']
     password = request.form['pass']

     sql = "INSERT INTO login_users (username, email, password) VALUES (%s, %s, %s);"
     insert = (username, email, password)
     cursor = con.cursor()
     cursor.execute(sql, insert)
     con.commit()
     cursor.execute("SELECT * FROM login_users where email LIKE {};".format(email))
     myuser = cursor.fetchall()
     session['username'] = myuser[0][0]
     return redirect(url_for('home'))

@app.route('/signout')
def signout():
     session.pop('username')
     return redirect('/')

news_url = 'https://newsapi.org/v2/everything?q=weather  alerts&apiKey=7f0493be147448a3b081fd0d368c034e'
 
def get_data(news_url):
     response = requests.get(news_url)
     resp = response.json()
     articles = resp['articles']
     return articles

articles = get_data(news_url)
         
@app.route("/news", methods=['POST', 'GET'])
def news():
     return render_template('news.html', articles = articles)

@app.route("/air_quality", methods=['POST', 'GET'])
def air_quality():
     cursor = conn.cursor()
     cursor.execute("SELECT DISTINCT ON (todays_date) * FROM public.weather_table ORDER BY todays_date desc, date_time desc NULLS LAST, city limit 1;")
     result = cursor.fetchall()
     place = result[0][0]
     country = result[0][1]
     latin = result[0][2]
     lotin = result[0][3]
     weather_url = requests.get(f'https://api.weatherbit.io/v2.0/current/airquality?lat={latin}&lon={lotin}&key=6d9d3d4fc0a34f569aa0c56e27eb7121')
     weather_data = weather_url.json()
     first = weather_data['data'][0]['aqi']
     return render_template('air_quality.html', first = first, place = place, country = country)


@app.route("/daily", methods=['POST', 'GET'])
def daily_result():
     cursor = conn.cursor()
     cursor.execute("SELECT DISTINCT ON (todays_date) * FROM public.weather_table ORDER BY todays_date desc, date_time desc NULLS LAST, city limit 1;")
     result = cursor.fetchall()
     print(result)
     place = result[0][0]
     latin = result[0][2]
     lontin = result[0][3]


     weather_url = requests.get(f'https://api.openweathermap.org/data/2.5/onecall?lat={latin}&lon={lontin}&exclude=current,minutely,hourly&appid=a728a370130711e2e8192f0aaa7ecb4b')
     weather_data = weather_url.json()
     day_1_temp = int(weather_data['daily'][0]['temp']['day']-273)
     day_1_con = weather_data['daily'][0]['weather'][0]['description']
     day_1_wind = weather_data['daily'][0]['wind_speed']
     day_1_icon =  weather_data['daily'][0]['weather'][0]['icon']
     day_2_temp = int(weather_data['daily'][1]['temp']['day']-273)
     day_2_con = weather_data['daily'][1]['weather'][0]['description']
     day_2_wind = weather_data['daily'][1]['wind_speed']
     day_2_icon =  weather_data['daily'][1]['weather'][0]['icon']
     day_2_date = weather_data['daily'][1]['dt']
     day_3_temp = int(weather_data['daily'][2]['temp']['day']-273)
     day_3_con = weather_data['daily'][2]['weather'][0]['description']
     day_3_wind = weather_data['daily'][2]['wind_speed']
     day_3_icon =  weather_data['daily'][2]['weather'][0]['icon']
     day_3_date = weather_data['daily'][2]['dt']
     day_4_temp = int(weather_data['daily'][3]['temp']['day']-273)
     day_4_con = weather_data['daily'][3]['weather'][0]['description']
     day_4_wind = weather_data['daily'][3]['wind_speed']
     day_4_icon =  weather_data['daily'][3]['weather'][0]['icon']
     day_4_date = weather_data['daily'][3]['dt']

     date2 = datetime.fromtimestamp(day_2_date).strftime('%a %d %b')
     date3 = datetime.fromtimestamp(day_3_date).strftime('%a %d %b')
     date4 = datetime.fromtimestamp(day_4_date).strftime('%a %d %b')
     
     return render_template('daily.html', place = place, day_1_temp = day_1_temp, day_1_con = day_1_con, day_1_wind = day_1_wind, day_2_temp = day_2_temp, day_2_con = day_2_con, day_2_wind = day_2_wind, day_3_temp = day_3_temp, day_3_con = day_3_con, day_3_wind = day_3_wind, day_1_icon = day_1_icon, day_2_icon = day_2_icon, day_3_icon = day_3_icon, day_4_icon = day_4_icon, day_4_temp = day_4_temp, day_4_con = day_4_con, day_4_wind = day_4_wind, date2 = date2, date3 = date3, date4 = date4)

@app.route("/hourly", methods=['POST', 'GET'])
def hourly_result():
     cursor = conn.cursor()
     cursor.execute("SELECT DISTINCT ON (todays_date) * FROM public.weather_table ORDER BY todays_date desc, date_time desc NULLS LAST, city limit 1;")
     result = cursor.fetchall()
     
     place = result[0][0]
     latin = result[0][2]
     lontin = result[0][3]

     weather_url = requests.get(f'https://api.openweathermap.org/data/2.5/onecall?lat={latin}&lon={lontin}&exclude=current,minutely,daily&appid=a728a370130711e2e8192f0aaa7ecb4b')
     weather_data = weather_url.json()
     day_1_temp = int(weather_data['hourly'][1]['temp']-273)
     day_1_con = weather_data['hourly'][1]['weather'][0]['description']
     day_1_time = weather_data['hourly'][1]['dt']
     day_1_icon =  weather_data['hourly'][1]['weather'][0]['icon']
     day_2_temp = int(weather_data['hourly'][4]['temp']-273)
     day_2_con = weather_data['hourly'][4]['weather'][0]['description']
     day_2_time = weather_data['hourly'][4]['dt']
     day_2_icon =  weather_data['hourly'][4]['weather'][0]['icon']
     day_3_temp = int(weather_data['hourly'][7]['temp']-273)
     day_3_con = weather_data['hourly'][7]['weather'][0]['description']
     day_3_time = weather_data['hourly'][7]['dt']
     day_3_icon =  weather_data['hourly'][7]['weather'][0]['icon']
     day_4_temp = int(weather_data['hourly'][10]['temp']-273)
     day_4_con = weather_data['hourly'][10]['weather'][0]['description']
     day_4_time = weather_data['hourly'][10]['dt']
     day_4_icon =  weather_data['hourly'][10]['weather'][0]['icon']

     time1 = datetime.fromtimestamp(day_1_time).strftime('%I %p')
     time2 = datetime.fromtimestamp(day_2_time).strftime('%I %p')
     time3 = datetime.fromtimestamp(day_3_time).strftime('%I %p')
     time4 = datetime.fromtimestamp(day_4_time).strftime('%I %p')

     return render_template('hourly.html', place = place, day_1_temp = day_1_temp, day_1_con = day_1_con, time1 = time1, day_2_temp = day_2_temp, day_2_con = day_2_con, time2 = time2, day_3_temp = day_3_temp, day_3_con = day_3_con, time3 = time3, day_1_icon = day_1_icon, day_2_icon = day_2_icon, day_3_icon = day_3_icon, day_4_temp = day_4_temp, day_4_con = day_4_con, time4 = time4, day_4_icon = day_4_icon)
          

@app.route("/contact", methods=['POST', 'GET'])
def contact_info():
     return render_template('contact.html')     

@app.route("/about", methods=['POST', 'GET'])
def about():
     return render_template('about.html')          

if __name__=='__main__':
    app.run(debug = True)         
