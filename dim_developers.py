
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
  user="root",
  password="Gtj#pC*QDwx[8rNt",
  port = 3306,
  database='etmam_dw_db' #DB Name
)

cursor = db.cursor()
def check_url_validity():
    status_code = urllib.request.urlopen(url).getcode()
    website_is_up = status_code == 200
    return website_is_up

def get_dim_developers():
    data = requests.get(url)
    return data.json()

def save_dim_developers(data,today,dt_string):
    for dim_dev in data:
        email = dim_dev['email']
        full_name = dim_dev['full_name']
        national_id = dim_dev['national_id']
        phone = dim_dev['phone']
        role = dim_dev['role']
        uid = dim_dev['uid']
        user_created = dim_dev['user_created']
        register_identity = dim_dev['register_identity']
        short_establish = dim_dev['short_establish']
        developer_type = dim_dev['developer_type']
        cr_name = dim_dev['cr_name']
        Beneficiry_cr = dim_dev['Beneficiry_cr']
        print("--user_created---")
        print(user_created)

        sql = "INSERT INTO dim_developers (user_id, national_id, full_name,email,phone,role,user_created,register_identity,short_establishment,developer_type,cr_name,beneficiry_cr)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (uid, national_id, full_name, email,phone,role,user_created,register_identity,short_establish,developer_type,cr_name,Beneficiry_cr)
        cursor.execute(sql, val)
        db.commit()

def sync_dim_developers(today,dt_string):
    data = get_dim_developers()
    print("___today___")
    print(today)
    print("date and time =", dt_string)
    save_dim_developers(data,today,dt_string)



def main():
    today = date.today()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    if check_url_validity() == True:
        sync_dim_developers(today,dt_string)


if __name__ == "__main__":
        main()
