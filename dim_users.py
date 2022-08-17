
import requests
import psycopg2
import pprint
import locale
import time
from datetime import datetime,date, timedelta
import json
import urllib
import mysql.connector as mysql
# from datetime import datetime, time ,timedelta
url = 'https://etmam-services.housing.gov.sa/user/dim-users'

db = mysql.connect(
  host="localhost",
  user="",
  password="",
  port = 3306,
  database='' #DB Name
)

cursor = db.cursor()



def check_url_validity():
    status_code = urllib.request.urlopen(url).getcode()
    website_is_up = status_code == 200
    return website_is_up

def get_dim_users():
    data = requests.get(url)
    return data.json()

def save_dim_users(data,today,dt_string):
    for dim_users in data:
        email = dim_users['email']
        full_name = dim_users['full_name']
        national_id = dim_users['national_id']
        phone = dim_users['phone']
        branch = dim_users['branch']
        role = dim_users['role']
        uid = dim_users['uid']
        sql = "INSERT INTO dim_users (user_id, national_id, full_name,email,phone,role,branch,refresh_date,refresh_datetime)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (uid, national_id, full_name, email,phone,role,branch,today,dt_string)
        cursor.execute(sql, val)
        db.commit()

def sync_dim_users(today,dt_string):
    data = get_dim_users()
    print("___today___")
    print(today)
    print("date and time =", dt_string)
    save_dim_users(data,today,dt_string)

def main():
    today = date.today()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    if check_url_validity() == True:
        sync_dim_users(today,dt_string)


if __name__ == "__main__":
        main()
