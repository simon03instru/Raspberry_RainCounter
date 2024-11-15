import time
import RPi.GPIO as GPIO
import signal
import sys
import paho.mqtt.publish as publish
import json
from datetime import datetime
import requests
import board
import busio
import adafruit_ina219
from gpiozero import CPUTemperature
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import io
import ftplib
import smbus
import mysql.connector

bus_number = 1

host = "localhost"
user = "root"
password = ""
database = ""

# Establish a connection
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Create a cursor
cursor = connection.cursor()

# Execute the SQL query to retrieve the row with the newest value
query = ""
cursor.execute(query)

# Fetch the row with the newest value
row = cursor.fetchone()

# Close the cursor and connection
cursor.close()
connection.close()

# Read params from sql table:
timestamp = row[1]
ntp = row[10]
ftp_mode = row[11]
url_ftp = row[12]
user_ftp = row[13]
pass_ftp = row[14]
periode_ftp = row[15]
http_mode = row[16]
url_http = row[17]
periode_http = row[18]
mqtt_mode = row[19]
url_mqtt = row[20]
port_mqtt = row[21]
user_mqtt = row[22]
pass_mqtt = row[23]
topic_mqtt = row[24]
periode_mqtt = row[25]

#insert variabel
url_http = url_http
url_mqtt = url_mqtt
url_ftp = url_ftp
periode_http = periode_http
periode_mqtt = periode_mqtt
periode_ftp = periode_ftp
username_mqtt = user_mqtt
password_mqtt = pass_mqtt
username_ftp = user_ftp
password_ftp = pass_ftp
topic_mqtt = topic_mqtt
port_mqtt = port_mqtt

http_mode = http_mode
mqtt_mode = mqtt_mode
ftp_mode = ftp_mode

resolusi = resolusi

NUM_RETRIES = 6

#Check if the voltage sensor is connected
ina219_address = 0x40
bus = smbus.SMBus(1)


#Parameter HTTP
#aws_url = (f"http://{url_http}/logger/write.php?dat=")

#path FTP file
local_file_path = '/home/'

running = True

auth = {
    "username": username_mqtt,
    "password": password_mqtt
}

retry_mqtt = 3
retry_interval = 2

#menit = 0
menit = 10
while(running):
    #waktu = datetime.now()
    waktu = datetime.utcnow()
    
    try:
        bus.read_byte_data(ina219_address, 0x00)
        i2c = busio.I2C(board.SCL, board.SDA)
        sensor = adafruit_ina219.INA219(i2c)
        voltage = sensor.bus_voltage
        voltage = "%2.2f" % voltage
        shunt = sensor.shunt_voltage
        shunt = "%2.2f" % shunt
        current = sensor.current
        current = "%2.2f" % current
    except IOError:
        voltage = -999
        shunt = -999
        current = -999
    except Exception as e:
        voltage = -999
        shunt = -999
        current = -999
        # Handle other exceptions if necessary
        #print("An error occurred:", str(e))
    
    cpu = CPUTemperature()
    suhu = cpu.temperature
    suhu = "%2.2f" % suhu

    with open('rain_update.txt','r') as file:
        curah_hujan = file.read().strip()
        
    data = {
        "date": waktu.strftime("%Y-%m-%d"),
        "time": waktu.strftime("%H:%M:00"),
        "rr": curah_hujan,
        "batt": voltage,
        "curr":  current,
        "log_temp": suhu,
        }

    if waktu.second  < 6:
        
        name_file = waktu.strftime("%Y%m%d")
        date_save = waktu.strftime("%Y-%m-%d")
        time_save = waktu.strftime("%H:%M:00")

        try:
            # Establish a connection
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )

            # Create a cursor
            cursor = connection.cursor()

            # SQL statement
            sql = "INSERT INTO data_tbl(date, time, rain, batt, current, temp_logger) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (date_save, time_save, curah_hujan, voltage, current, suhu)

            # Execute SQL
            cursor.execute(sql, val)

            # Commit the transaction
            connection.commit()

        except mysql.connector.Error as e:
            print(f"Error: {e}")
            # Handle any errors as needed

        finally:
            # Close the cursor and connection to release resources
            cursor.close()
            connection.close()

        time.sleep(5)

        timestr = waktu.strftime('%d%m%Y%H%M00')
        waktuftp = waktu.strftime('%Y%m%d%H%M')

        if http_mode == True:
            if waktu.minute % periode_http == 0:
                try:
                    for _ in range(NUM_RETRIES):
                        try:
                            response = requests.get(f"{url_http};{timestr};{curah_hujan};{voltage};{current};{suhu}",timeout=5)
                            if response.status_code in [200,404]:
                                break
                        except requests.exceptions.ConnectionError:
                            pass

                    if response is not None and response.status_code == 200:
                        pass

                           
        if mqtt_mode == True:    
            if waktu.minute % periode_mqtt == 0:
                for i in range(retry_mqtt):
                    try:
                        publish.single(topic_mqtt, json.dumps(data), retain = True, hostname = url_mqtt, port = port_mqtt, auth=auth)
                        break
    
        if ftp_mode == True:
            if waktu.minute % periode_ftp == 0:
                file = io.BytesIO()
                file_wrapper = io.TextIOWrapper(file, encoding='utf-8')
                file_wrapper.write(f"{timestr};{curah_hujan};{voltage};{current};{suhu}")
                file_wrapper.seek(0)
                try:
                    with ftplib.FTP() as ftp:
                        ftp.connect(host=url_ftp, port=21, timeout=10)
                        ftp.login(user=username_ftp, passwd=password_ftp)
                        ftp.storbinary(f"STOR{waktuftp}.txt", file)
                except Exception:
                    pass

        if waktu.hour == 0 and waktu.minute == 0:
            count = 0
    time.sleep(1)
    #menit += 1



