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
url = 'https://etmam-services.housing.gov.sa/ar/user/dim-state?date=all'

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

def get_dim_state():
    data = requests.get(url)
    return data.json()

def save_dim_state(data,today,dt_string):
    for dim_state in data:
        nid = dim_state['ID']
        Application_Id = dim_state['Application_Id']
        Age = dim_state['Age']
        Stage = dim_state['Stage']
        old_State = dim_state['old_State']
        sate_machine_name = dim_state['sate_machine_name']
        Satge_complation_date = dim_state['Satge_complation_date']
        Comment = dim_state['Comment']
        responsible_employee = dim_state['responsible_employee']
        employee_username = dim_state['employee_username']
        sql = "INSERT INTO dim_state (nid,Application_Id,Age,Stage,old_State,sate_machine_name,Satge_complation_date,Comment,responsible_employee,employee_username)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (nid,Application_Id,Age,Stage,old_State,sate_machine_name,Satge_complation_date,Comment,responsible_employee,employee_username)
        cursor.execute(sql, val)
        db.commit()

def sync_dim_state(today,dt_string):
    data = get_dim_state()
    print("___today___")
    print(today)
    print("date and time =", dt_string)
    save_dim_state(data,today,dt_string)

def main():
    today = date.today()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    if check_url_validity() == True:
        sync_dim_state(today,dt_string)


if __name__ == "__main__":
        main()
