import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests

def get_covid19_report_today():
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    data = response.json()
    with open('data.json', 'w') as f:
        json.dump(data, f)
    return data

def create_covid19_report_table():
    sqlite_hook = SqliteHook(sqlite_conn_id='playgroundDB')
    create = """
             CREATE TABLE  "Covid19Report" (
	            "confirmed"	INTEGER,
	            "recovered"	INTEGER,
	            "hospitalized"	INTEGER,
	            "deaths"	INTEGER,
	            "new_confirmed"	INTEGER,
	            "new_recovered"	INTEGER,
	            "new_hospitalized"	INTEGER,
	            "new_deaths"	INTEGER,
	            "update_date"	TEXT,
                "source"    TEXT,
                "dev_by"    TEXT,
                "sever_by"  TEXT);
             """
    sqlite_hook.run(create)

def save_data_into_db():
    sqlite_hook = SqliteHook(sqlite_conn_id='playgroundDB')
    with open('data.json') as f:
        data = json.load(f)
    insert = """
            INSERT INTO Covid19Report (
                confirmed,
                recovered,
                hospitalized,
                deaths,
                new_confirmed,
                new_recovered,
                new_hospitalized,
                new_deaths,
                update_date,
                source,
                dev_by,
                sever_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
             """
    sqlite_hook.run(insert,parameters=(
            data['Confirmed'],
            data['Recovered'],
            data['Hospitalized'],
            data['Deaths'],
            data['NewConfirmed'],
            data['NewRecovered'],
            data['NewHospitalized'],
            data['NewDeaths'],
            datetime.strptime(data['UpdateDate'],'%d/%m/%Y %H:%M'),
            data['Source'],
            data['DevBy'],
            data['SeverBy']))

def return_line_noti():
    url = 'https://notify-api.line.me/api/notify'
    token = 'enter your line notify token'
    headers = {
                'content-type':
                'application/x-www-form-urlencoded',
                'Authorization':'Bearer '+token
            }
    msg = "Covid19 Data pipeline is done."
    return requests.post(url, headers=headers , data = {'message':msg})


default_args = {
    'owner': 'akarapon',
    'start_date': datetime(2021, 2, 1),
    'end_date':datetime(2021, 2, 28),
    'email': ['akarapon2541.work@gmail.com'],
    }

with DAG('covid19_data_pipeline',
         schedule_interval='30 6 * * *',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_covid19_report_today',
        python_callable=get_covid19_report_today
    )

    # t2 = PythonOperator(
    #     task_id='create_data_table',
    #     python_callable=create_covid19_report_table
    # )

    t3 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t4 = PythonOperator(
        task_id='send_notification',
        python_callable=return_line_noti
    )

# If No such a Table: Uncomment Below
# t1 >> t2 >> t3 >> t4

t1 >> t3 >> t4
