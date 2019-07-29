#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import sqlite3
import datetime
from functools import partial
from threading import Timer
import sys
import time
import os
import dropbox

backup_timer = None

######## 
# MQTT handling code
def on_connect(client, user_data, flags, rc):
  print('Connected with result code "{}"'.format(rc))

  client.subscribe('owntracks/#')

def on_message(db, client, userdata, msg):
  print('{} -> {}'.format(msg.topic, msg.payload))
  db.add_location_entry(msg.topic, msg.payload)

def on_disconnect(database):
  database.close()
  if backup_timer is not None:
    backup_timer.cancel()

########
# sqlite handling code
class LocationDatabase(object):
  def __init__(self, database_file):
    self.conn = sqlite3.connect(database_file)
    #self.conn.set_trace_callback(print)
    self.init_tables_if_required()

  def init_tables_if_required(self):
    cursor = self.conn.cursor()
    cursor.execute('SELECT * FROM sqlite_master WHERE tbl_name = "location_data"')
    res = cursor.fetchone()

    if res is None:
      cursor.execute('CREATE TABLE location_data(topic VARCHAR(255), time DATETIME, payload TEXT)')
      self.conn.commit()
    cursor.close

  def add_location_entry(self, topic, payload):
    cursor = self.conn.cursor()
    cursor.execute('INSERT INTO location_data VALUES (?, ?, ?)', (topic, datetime.datetime.now(), payload))
    self.conn.commit()
    cursor.close()

  def backup(self, backup_file_name):
    backup_conn = sqlite3.connect(backup_file_name)
    self.conn.backup(backup_conn)
    backup_conn.close()

  def close(self):
    print('Disconnecting from database')
    conn.close()

def trigger_backup(database, dropbox):
  filename = 'backup-' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.sqlite'
  print('Backing up to file {}... '.format(filename), end='')
  database.backup(filename)
  upload_file_to_dropbox(dropbox, filename)
  print('done')
  t = Timer(60 * 60 * 24, lambda: trigger_backup(database, dropbox))
  backup_timer = t
  t.start()

def upload_file_to_dropbox(dropbox, filename):
  with open(filename, 'rb') as f:
    data = f.read()
  dropbox.files_upload(data, '/{}'.format(filename))

if __name__ == '__main__':
  if len(sys.argv) != 4:
    print('Usage: <database file> <server name> <port number>')
    sys.exit(1)

  database_file = sys.argv[1]
  server_name = sys.argv[2]
  server_port = int(sys.argv[3])

  if 'DROPBOX_ACCESS_KEY' not in os.environ:
    print('Please set DROPBOX_ACCESS_KEY to your Dropbox access key.')
    sys.exit(2)

  dropbox_app_key = os.environ['DROPBOX_ACCESS_KEY']

  database = LocationDatabase(database_file)
  dropbox = dropbox.Dropbox(dropbox_app_key)

  client = mqtt.Client()
  client.on_connect = on_connect
  client.on_message = partial(on_message, database)
  client.on_disconnect = partial(on_disconnect, database)
  client.connect(server_name, server_port, 60)

  t = Timer(10, lambda: trigger_backup(database, dropbox))
  backup_timer = t
  t.start()

  client.loop_forever()
