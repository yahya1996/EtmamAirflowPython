
import requests
import psycopg2
import pprint
import locale
import time
from datetime import datetime,date, timedelta
import json
import urllib
import mysql.connector as mysql
#from datetime import datetime, time ,timedelta
url = 'https://etmam-services.housing.gov.sa/ar/user/fact-app?date=all'

db = mysql.connect(
   host="10.0.4.2",
  user="airflow_us",
  password="Yahyaayyoub1996@#$",
  port = 3306,
  database='etmam_tableau' #DB Name
)

cursor = db.cursor()


def check_url_validity():
    status_code = urllib.request.urlopen(url).getcode()
    website_is_up = status_code == 200
    return website_is_up

def get_fact_data():
    data = requests.get(url)
    return data.json()

def save_fact_data(data,today,dt_string):
    for fact_data in data:
        nid = fact_data['nid']
        application_id = fact_data['project_id']
        service_type = fact_data['type']
        service_name = fact_data['type_name']
        region = fact_data['Region']
        city = fact_data['City']
        create_date = fact_data['created_date']
        state = fact_data['State']
        dateLastState = fact_data['dateLastState']

        sql = "INSERT INTO Fact_applications (nid,application_id,service_id,service_name,region,city,created_date,current_state,final_complation_date)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (nid ,application_id,service_type,service_name,region,city,create_date,state,dateLastState)
        cursor.execute(sql, val)
        db.commit()

def sync_fact_data(today,dt_string):
    data = get_fact_data()
    print("___today___")
    print(today)
    print("date and time =", dt_string)
    save_fact_data(data,today,dt_string)

def main():
    today = date.today()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    if check_url_validity() == True:
        sync_fact_data(today,dt_string)


if __name__ == "__main__":
        main()
