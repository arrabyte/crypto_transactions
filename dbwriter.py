# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package dbwriter
# Writer specialization to write data on sqlite database

from writer import Writer
from writer import transaction_fields
import sqlite3

class DbWriter(Writer):
  def __init__(self, db_conn_instance):
    self.db_conn_instance = sqlite3.connect('trades.db')
    cursor = db_conn_instance.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS trades
      ({0} number,
      {1} datetime,
      {2} text,
      {3} text,
      {4} number,
      {5} text,
      {6} text,
      PRIMARY KEY ({0}, {1}))'''
      .format(transaction_fields.id,
              transaction_fields.datetime,
              transaction_fields.currency,
              transaction_fields.operation,
              transaction_fields.amount,
              transaction_fields.exchange,
              transaction_fields.hash))

  # check_duplicated return True is a duplicated was not found
  def check_duplicated(self, row, cursor):
    query = '''SELECT id FROM trades WHERE id = {0} and datetime = "{1}"'''.format(row[transaction_fields.id], row[transaction_fields.datetime])
    result_set = cursor.execute(query)
    return result_set.arraysize > 0

  def write_rows(self, rows):
    cursor = self.db_conn_instance.cursor()
    for row in rows:
      if self.check_duplicated(row, cursor):
        print("warning -- element id: {0} datetime: {1} already exists".format(row[transaction_fields.id], row[transaction_fields.datetime]))
      else:
        query = "INSERT INTO trades VALUES ({0}, '{1}', '{2}', '{3}', {4}, '{5}', '{6}')".format(
          row[transaction_fields.id],
          row[transaction_fields.datetime],
          row[transaction_fields.currency],
          row[transaction_fields.operation],
          row[transaction_fields.amount],
          row[transaction_fields.exchange],
          row[transaction_fields.hash])
        cursor.execute(query)

  def finalize(self):
    self.db_conn_instance.commit()
    self.db_conn_instance.close()
