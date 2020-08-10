# -*- coding: utf-8 -*- 

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..\..\Python'))
               
import DB_Insert_Transaction_With as air


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,10), # 적당히 조절
	schedule_interval = '30 * * * *')  # 적당히 조절  
  # M,H,DM,MY,DW,Y
  # Minute            ( range 0-59 )
  # Hour              ( range 0-23 )
  # Day of the Month  ( range 1-31 )
  # Month of the Year ( range 1-12 )
  # Day of the Week   ( range 1-7  ) standing for Monday
  # Year              ( range 1900-3000 )

def etl():
	 # 여기에 코드를 기록
  link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
  data = air.extract(link)
  lines = air.transform(data)
  air.load(lines)   

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
  
# task가 하나 밖에 없는 경우 아무 것도 하지 않아도 그냥 실행됨
if __name__ == "__main__":    
    etl()
  # pass