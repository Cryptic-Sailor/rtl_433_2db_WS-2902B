#!/usr/bin/python
# import sys
import subprocess
import time
import threading
import json
import Queue
import mysql.connector
from mysql.connector import errorcode

config = {
    'user': '',
    'password': '',
    'host': '127.0.0.1',
    'database': '',
    'raise_on_warnings': True,
}


class AsynchronousFileReader(threading.Thread):
    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()


def replace(string):
    while '  ' in string:
        string = string.replace('  ', ' ')
    return string


def startsubprocess(command):
    '''
    Example of how to consume standard output and standard error of
    a subprocess asynchronously without risk on deadlocking.
    '''
    print "\n\nStarting sub process " + command + "\n"
    # Launch the command as subprocess.

    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Launch the asynchronous readers of the process' stdout and stderr.
    stdout_queue = Queue.Queue()
    stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
    stdout_reader.start()
    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.start()
    # do database stuff init
    try:
        print("Connecting to database")
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists, please create it before using this script.")
            print("Tables can be created by the script.")
        else:
            print(err)
    reconnectdb = 0  # if 0 then no error or need ro be reconnected
    # else:
    # cnx.close()
    cursor = cnx.cursor()
    TABLES = {}
    TABLES['SensorData'] = (
        "CREATE TABLE `SensorData` ("
        "   `time`          TIMESTAMP NOT NULL PRIMARY KEY,"
        "   `model`         VARCHAR(16) NOT NULL,"
        "   `id`            INTEGER  NOT NULL,"
        "   `battery_ok`    BIT  NOT NULL,"
        "   `temperature_C` NUMERIC(10,3) NOT NULL,"
        "   `humidity`      INTEGER  NOT NULL,"
        "   `wind_dir_deg`  INTEGER  NOT NULL,"
        "   `wind_avg_m_s`  NUMERIC(10,3) NOT NULL,"
        "   `wind_max_m_s`  NUMERIC(10,3) NOT NULL,"
        "   `rain_mm`       NUMERIC(10,3) NOT NULL,"
        "   `uv`            INTEGER  NOT NULL,"
        "   `uvi`           INTEGER  NOT NULL,"
        "   `light_lux`     NUMERIC(10,3) NOT NULL,"
        "   `mic`           VARCHAR(3) NOT NULL)")
    for name, ddl in TABLES.iteritems():
        try:
            print("Checking table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table seams to exist, no need to create it.")
            else:
                print(err.msg)
        else:
            print("OK")
    add_sensordata = ("INSERT INTO SensorData "
                      "(time,model,id,battery_ok,temperature_C,humidity,wind_dir_deg,wind_avg_m_s,wind_max_m_s,rain_mm,uv,uvi,light_lux,mic)"
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    # do queue loop, entering data to database
    # Check the queues if we received some output (until there is nothing more to get).
    while not stdout_reader.eof() or not stderr_reader.eof():
        # Show what we received from standard output.
        while not stdout_queue.empty():
            line = stdout_queue.get()
            print repr(line)
            load=0
            try:
                l = json.loads(line.rstrip('\n'))
                load = 1
            except:
                print ("error")
            if load == 1:
                try:
                    if reconnectdb == 1:
                        print("Trying reconnecting to database")
                        cnx.reconnect()
                        reconnectdb = 0
                    if l['model'] == "Fineoffset-WH65B":
                        sensordata = (l['time'],l['model'],l['id'],l['battery_ok'],l['temperature_C'],l['humidity'],l['wind_dir_deg'],l['wind_avg_m_s'],l['wind_max_m_s'],l['rain_mm'],l['uv'],l['uvi'],l['light_lux'],l['mic'])  # shit go here nigga
                        cursor.execute(add_sensordata, sensordata)
                        # Make sure data is committed to the database
                        print("committing")
                        cnx.commit()
                except mysql.connector.Error as err:
                    print (err)
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("Table seams to exist, no need to create it.")
        while not stderr_queue.empty():
            line = replace(stderr_queue.get())
            print str(line)

    # Sleep a bit before asking the readers again.
    time.sleep(.1)

    # Let's be tidy and join the threads we've started.
    try:
        cursor.close()
        cnx.close()
    except:
        pass
    stdout_reader.join()
    stderr_reader.join()

    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()


if __name__ == '__main__':
    # The main flow:
    # check if database is present, create tablesif no tables present

    startsubprocess("rtl_433")
    print("Closing down")
