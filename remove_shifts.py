#!/home/krejcarek/petris/bin/python3

import argparse

import psycopg2
from psycopg2 import Error

import datetime
from datetime import date, timedelta

# Create parser
parser = argparse.ArgumentParser()

# Default variables
today = date.today()
weekday = date(today.year, today.month, today.day).weekday()
monday = date(today.year, today.month, today.day)+(timedelta(days=14-weekday)) # Monday in 2 weeks
# Don't need friday, but keeping it here in case I want it later
friday = date(today.year, today.month, today.day)+(timedelta(days=14+4-weekday)) # Friday in 2 weeks

# Add arguments, create variables
parser.add_argument('--name', type = str, default = 'Unassigned')
parser.add_argument('--start_date', type = str, default = monday.isoformat())
parser.add_argument('--end_date', type = str)
parser.add_argument('--calendar_spot_name', choices=['Waisman_Scanners', 'Cancellations'], required = True, help = 'Waisman_Scanners, or Cancellations')
args = parser.parse_args()

# Create variables
employee_name = args.name
start_date_string = args.start_date
end_date_string = args.end_date
if start_date_string.lower() == "all" or end_date_string.lower() == "all":
    start_date = "all"
    end_date ="all"
else:
    start_date_list = start_date_string.split("-")
    start_date = datetime.date(int(start_date_list[0]), int(start_date_list[1]), int(start_date_list[2]))
    end_date = None
    if end_date_string is None:
        end_date = start_date + timedelta(days=4)
    else:
        end_date = datetime.date(int(end_date_string.split("-")[0]), int(end_date_string.split("-")[1]), int(end_date_string.split("-")[2]))
    # Check
    if start_date > end_date:
        raise ValueError('start_date must come before end_date.')
calendar_spot_name = args.calendar_spot_name

# Print info
print ("\n", 'Name:', employee_name)
print ('Shift:', start_date, 'through', end_date)
print ('Calendar Spot Name:', calendar_spot_name, "\n")


# SQL Queries

try:
    # Connect to an existing database
    connection = psycopg2.connect(user="petris", password = "petris", host = "127.0.0.1", port = "5432", database = "petris")
    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    #print("\nPostgreSQL server information:")
    #print(connection.get_dsn_parameters())

    # Gets employee_id from employee name input
    sql_select_employee_id = "(SELECT id FROM employee WHERE name = '" + employee_name + "')"
    # Adds specified start and end dates
    sql_and_between_dates = " AND shift.startdatetime >= '" + str(start_date) + " 00:00:00' AND shift.enddatetime <= '" + str(end_date) + " 23:59:59'"
    # Selects all shifts for employee between dates
    if employee_name != "Unassigned":
        sql_select_shifts = ("SELECT id FROM shift WHERE employee_id = " + sql_select_employee_id + sql_and_between_dates)
    else:
        sql_select_shifts = ("SELECT id FROM shift WHERE employee_id IS NULL" + sql_and_between_dates)
    # Counts the shifts
    if employee_name != "Unassigned":
        sql_select_count_shifts = ("SELECT COUNT(*) FROM shift WHERE employee_id = " + sql_select_employee_id + sql_and_between_dates)
    else:
        sql_select_count_shifts = ("SELECT COUNT(*) FROM shift WHERE employee_id IS NULL " + sql_and_between_dates)
    # Counts the number of employees with the specific name
    sql_count_employee_name = ("SELECT COUNT(id) FROM employee WHERE name = '" + employee_name + "'")
    # Selects shiftids that match the ids in shift
    sql_select_shiftid_from_shiftrequiredskillset = ("SELECT shiftid FROM shiftrequiredskillset WHERE shiftid IN (" + sql_select_shifts + ")")
    # Deletes shifts from shiftrequiredskillset
    sql_delete_from_shiftrequiredskillset = ("DELETE FROM shiftrequiredskillset WHERE  shiftid IN (" + sql_select_shifts + ")")
    # Deletes shifts
    if employee_name != "Unassigned":
        sql_delete_from_shift = ("DELETE FROM shift WHERE shift.employee_id = " + sql_select_employee_id + sql_and_between_dates)
    else:
        sql_delete_from_shift = ("DELETE FROM shift WHERE shift.employee_id IS NULL " + sql_and_between_dates)
    # Checks that there is an employee with that name
    cursor.execute(sql_count_employee_name)
    record = cursor.fetchall()
    if (record[0])[0] > 0 or employee_name == "Unassigned":
        # Selects and counts the shifts
        cursor.execute(sql_select_shifts)
        record = cursor.fetchall()
        print("Results: ", record, "\n")
        cursor.execute(sql_select_count_shifts)
        record = cursor.fetchall()
        print("Deleting", (record[0])[0], "row(s)")
        # Delete rows from shiftrequiredskillset and shift
        cursor.execute(sql_delete_from_shiftrequiredskillset)
        cursor.execute(sql_select_shiftid_from_shiftrequiredskillset)
        record = cursor.fetchall()
        print("\nShiftrequiredskillset: ", record)
        cursor.execute(sql_delete_from_shift)
        cursor.execute(sql_select_shifts)
        record = cursor.fetchall()
        print("\nShift: ", record)
    else:
        print("Employee name not found")
    connection.commit()
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL: ", error)
    connection.rollback()
finally:
    if (connection):

        cursor.close()
        connection.close()
        print("\nPostgreSQL connection is closed \n")


