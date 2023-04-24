import time


from cassandra.cluster import Cluster
import csv
import datetime
import requests

# Set up the Cassandra connection
cluster = Cluster(['localhost'])
session = cluster.connect('my_keyspace')
table_name = "my_table"

# query the data generated from 2023.03.08 00:00:00 to now
# time is of the format: 2018-01-01 00:00:00 (year-month-day hour:minute:second)
query = "SELECT * FROM " + table_name + " WHERE metric in ('v.building.A001A.elec.hourly', " \
                                        "'v.building.A001B.elec.hourly', 'v.building.A001C.elec.hourly', " \
                                        "'v.building.A001D.elec.hourly', 'v.building.A001E.elec.hourly', " \
                                        "'v.building.A002A.elec.hourly', 'v.building.A002B.elec.hourly', " \
                                        "'v.building.A002C.elec.hourly', 'v.building.A002D.elec.hourly', " \
                                        "'v.building.A002E.elec.hourly')" + " AND time > '" + \
        str(datetime.datetime(2023, 3, 8, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")) + "'"
result = session.execute(query)
# filename should conclude the data when the script is executed and historical data
filename = "2023.03.08" + "+" + str(datetime.datetime.now().date()) + ".csv"
with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(result.column_names)
    for row in result:
        writer.writerow(row)
# initiate a http request to another server
# it should be a POST request which includes the file
# the server should return a response with status code 200
# if the response is not 200, the script should retry 3 times
# if the response is 200, the script should terminate
# the script should wait 5 seconds before retrying
for _ in range(3):
    try:
        request = requests.post('http://10.106.132.240:8080/file', files={'file': open(filename, 'rb')})
        if request.status_code == 200:
            time.sleep(5)
            break
        else:
            print("retrying")
            time.sleep(5)
            continue
    except:
        print("retrying")
        time.sleep(5)
        continue


# Close the Cassandra connection
session.shutdown()
cluster.shutdown()

while True:
    # Set up the Cassandra connection
    cluster = Cluster(['localhost'])
    session = cluster.connect('my_keyspace')
    table_name = "my_table"

    list = ["v.building.A001A.elec.hourly",
            "v.building.A001B.elec.hourly",
            "v.building.A001C.elec.hourly",
            "v.building.A001D.elec.hourly",
            "v.building.A001E.elec.hourly",
            "v.building.A002A.elec.hourly",
            "v.building.A002B.elec.hourly",
            "v.building.A002C.elec.hourly",
            "v.building.A002D.elec.hourly",
            "v.building.A002E.elec.hourly"]

    for i in list:
        # query the data generated in last 24 hours
        # time is of the format: 2018-01-01 00:00:00 (year-month-day hour:minute:second)
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        query = "SELECT * FROM " + table_name + " WHERE metric in ('" + i + "')" + " AND time > '" + str(
            yesterday.strftime("%Y-%m-%d %H:%M:%S")) + "'"
        print(query)
        result = session.execute(query)
        # filename should conclude the metric name and the data when the script is executed
        filename = i + "+" + str(datetime.datetime.now().date()) + ".csv"
        print(filename)
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(result.column_names)
            for row in result:
                writer.writerow(row)
        # initiate a http request to another server
        # it should be a POST request which includes the file
        # the server should return a response with status code 200
        # if the response is not 200, the script should retry 3 times
        # if the response is 200, the script should terminate
        # the script should wait 5 seconds before retrying
        for _ in range(3):
            try:
                request = requests.post('http://10.106.132.240:8080/file', files={'file': open(filename, 'rb')})
                if request.status_code == 200:
                    time.sleep(5)
                    break
                else:
                    print("retrying")
                    time.sleep(5)
                    continue
            except:
                print("retrying")
                time.sleep(5)
                continue

    # Close the Cassandra connection
    session.shutdown()
    cluster.shutdown()
    # wait for 24 hours before executing the script again
    time.sleep(86400)
