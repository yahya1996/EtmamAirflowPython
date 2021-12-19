
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
url = 'https://etmam-services.housing.gov.sa/user/dim-applications'

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

def get_dim_applications():
    data = requests.get(url)
    return data.json()

def save_dim_applications(data,today,dt_string):

    for dim_applications in data:
        nid = dim_applications['nid']
        application_id = dim_applications['application_id']
        service_type = dim_applications['service_type']
        company_name = dim_applications['company_name']
        project_name = dim_applications['project_name']
        area_m2 = dim_applications['area_m2']
        project_type = dim_applications['project_type']
        region = dim_applications['region']
        city = dim_applications['city']
        branch = dim_applications['branch']
        user_id = dim_applications['user_id']
        create_date = dim_applications['create_date']
        state = dim_applications['state']
        days = dim_applications['days']
        #post_date = date.strptime(create_date,'%Y-%m-%d')
        #units = dim_applications['units'] use Units After Malik Solve it


        sql = "INSERT INTO dim_applications (application_id, application_number, service_name,company_name,project_title,land_area_m2,project_type,region,city,branch,developer_id,post_date,duration_days,approve_reject_flag,refresh_date,refresh_datetime)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (application_id, nid,service_type, company_name, project_name,area_m2,project_type,region,city,branch,user_id,create_date,days,state,today,dt_string)
        cursor.execute(sql, val)
        db.commit()

def sync_dim_applications(today,dt_string):
    data = get_dim_applications()
    print("___today___")
    print(today)
    print("date and time =", dt_string)
    save_dim_applications(data,today,dt_string)

def main():
    today = date.today()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    if check_url_validity() == True:
        sync_dim_applications(today,dt_string)


if __name__ == "__main__":
        main()
