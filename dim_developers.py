
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
url = 'https://etmam-services.housing.gov.sa/user/dim-developers'

db = mysql.connect(
  host="localhost",
  user="newuser",
  password="password",
  port = 3306,
  database='etmam_dw'
)

cursor = db.cursor()



def check_url_validity():
    status_code = urllib.request.urlopen(url).getcode()
    website_is_up = status_code == 200
    return website_is_up

def get_dim_developers():
    data = requests.get(url)
    return data.json()

def save_dim_developers(data):
    for dim_dev in data:
        email = dim_dev['email']
        full_name = dim_dev['full_name']
        national_id = dim_dev['national_id']
        phone = dim_dev['phone']
        registrar_type = dim_dev['registrar_type']
        role = dim_dev['role']
        uid = dim_dev['uid']
        sql = "INSERT INTO dim_developers (user_id, national_id, full_name,email,phone,role)  VALUES (%s,%s,%s,%s,%s,%s,%s)"
        val = (1, "test")
        cursor.execute(uid, national_id, full_name, email,phone,role)
        db.commit()

def sync_dim_developers(today):
    data = get_dim_developers()
    print("___today___")
    print(today)


    #save_dim_developers(data)

    print(json.dumps(data, indent=4, sort_keys=True))

    #save_dim_developers(data)

def main():
    today = date.today()
    if check_url_validity() == True:
        sync_dim_developers(today)


if __name__ == "__main__":
        main()
