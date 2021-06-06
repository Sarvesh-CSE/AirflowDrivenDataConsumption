import requests
import json
from datetime import datetime, timedelta
import os
import cgi

def get_weather():
    """
	Query openweathermap.com's API and to get the weather for
	Varanasi and then dump the json to the /src/data/ directory 
	with the file name "<today's date>.json"
	""" 

    url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=imperial&appid=a728a370130711e2e8192f0aaa7ecb4b'
    
    #frm = cgi.FieldStorage()
    #city = frm.getvalue('location')
    city = 'Varanasi, IN'
    result = requests.get(url.format(city))
   
    if result.status_code == 200 :

        json_data= result.json()
        file_name= str(datetime.now().date()) + '.json'
        tot_name= os.path.join(os.path.dirname(__file__), 'data', file_name)

        with open(tot_name, 'w') as outputfile:
              json.dump(json_data, outputfile)

    # else :
 
        #print("error in API call.")       

if __name__ == "__main__":
    get_weather()       


