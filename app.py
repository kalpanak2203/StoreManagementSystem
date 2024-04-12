import json
from flask import Flask, jsonify, request
import pandas
import sqlite3
from datetime import datetime
from dateutil import tz


app = Flask(__name__)

employees = [
 { 'id': 1, 'name': 'Ashley' },
 { 'id': 2, 'name': 'Kate' },
 { 'id': 3, 'name': 'Joe' }
]


nextEmployeeId = 4
3


def instance_method():
  conn = sqlite3.connect(':memory:')
  storeStatusFile = pandas.read_csv('datafiles/store-status.csv')
  storeStatusFile.to_sql('Store_Status', conn, index=False, if_exists='replace')

  storeBussinessHoursFile = pandas.read_csv('datafiles/bussiness-hours.csv')
  storeBussinessHoursFile.to_sql('Store_Timings', conn, index=False, if_exists='replace')

  storeTimezoneFile = pandas.read_csv('datafiles/bq-results-20230125-202210-1674678181880.csv')
  storeTimezoneFile.to_sql('Store_Timezone', conn, index=False, if_exists='replace')

  # query = """
  # SELECT * FROM Store_Status
  # WHERE store_id = "8419537941919820732";
  # """
  query = """
  CREATE TABLE output
  AS (
    SELECT store_id FROM Store_Status,
    (SELECT timestamp_utc 
    WHERE timestamp_utc BETWEEN SELECT start_time_local FROM Store_Timings WHERE store_id == Store_Timings.store.id AND end_time_local FROM Store_Timings WHERE store_id == Store_Timings.store.id)
    );
  """

  result = pandas.read_sql_query(query, conn)
  print(result)

@app.route('/trigger_report', methods=['GET'])
def get_employees():
  instance_method()
  return jsonify(employees)

@app.route('/employees/<int:id>', methods=['GET'])
def get_employee_by_id(id: int):
 employee = get_employee(id)
 if employee is None:
   return jsonify({ 'error': 'Employee does not exist'}), 404
 return jsonify(employee)

def get_employee(id):
 return next((e for e in employees if e['id'] == id), None)

def employee_is_valid(employee):
 for key in employee.keys():
   if key != 'name':
    return False
 return True



if __name__ == '__main__':
   app.run(port=5000)