# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package ftx_importer
# this package defines functions to import data exported from FTX exchage

from operator import contains
import sys
import datetime
from typing import Dict
import dateutil.parser
from collections import namedtuple
from numpy import empty
from pandas import array
from writer import Writer
from writer import transaction_fields
from csvwriter import CsvWriter
from dbwriter import DbWriter
import sqlite3
import csv

#"id","time","market","side","type","size","price","total","fee","feeCurrency"
#"5869147485","2021-12-27T19:17:46.252096+00:00","AVAX/USD","buy","Limit","5.8","118.7","688.46","0.001102","AVAX"
#ftx_row_fields = {namedtuple("id", 0), "time": 1, "market":2 , "side": 3, "type": 4, "size": 5, "price": 6, "total": 7, "fee": 8, "feeCurrency": 9}
def ftx_import_trades(filename, writer: Writer):
  FieldsPos = namedtuple("FieldsPos", "id time market side type size price total fee feeCurrency")
  ftx_row_fields_pos = FieldsPos(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

  with open(filename, newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')

    # skip header
    next(spamreader)

    for row_id, row in enumerate(spamreader, 1):
      currencies = row[ftx_row_fields_pos.market]
      if currencies.find("-PERP") != -1:
        currency_A = currencies
        currency_B = "USD"
      else:
        currencies_array = currencies.split("/")
        currency_A = currencies_array[0]
        currency_B = currencies_array[1]

      rows = [
        {
          transaction_fields.id: row[ftx_row_fields_pos.id].strip('"'),
          transaction_fields.datetime: dateutil.parser.isoparse(row[ftx_row_fields_pos.time]),
          transaction_fields.currency: currency_A.strip('"'),
          transaction_fields.operation: row[ftx_row_fields_pos.side].strip('"'),
          transaction_fields.amount: row[ftx_row_fields_pos.size].strip('"'),
          transaction_fields.exchange: "ftx",
          transaction_fields.hash : ""
        },
        {
          transaction_fields.id: row[ftx_row_fields_pos.id].strip('"'),
          transaction_fields.datetime: dateutil.parser.isoparse(row[ftx_row_fields_pos.time]),
          transaction_fields.currency: currency_B.strip('"'),
          transaction_fields.operation: "buy" if row[ftx_row_fields_pos.side].strip('"') == "sell" else "sell",
          transaction_fields.amount: row[ftx_row_fields_pos.total].strip('"'),
          transaction_fields.exchange: "ftx",
          transaction_fields.hash: ""
        },
        {
          transaction_fields.id: row[ftx_row_fields_pos.id].strip('"'),
          transaction_fields.datetime: dateutil.parser.isoparse(row[ftx_row_fields_pos.time]),
          transaction_fields.currency: row[ftx_row_fields_pos.feeCurrency],
          transaction_fields.operation: "fee",
          transaction_fields.amount: row[ftx_row_fields_pos.fee],
          transaction_fields.exchange: "ftx",
          transaction_fields.hash : ""
        },
      ]

      writer.write_rows(rows)

def ftx_import_deposit(filename, writer: Writer):
  FieldsPos = namedtuple("FieldsPos", "id time currency qta trans_hash")
  ftx_row_fields_pos = FieldsPos(0, 1, 2, 3, 6)

  with open(filename, newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')

    # skip header
    next(spamreader)

    for row_id, row in enumerate(spamreader, 1):

      rows = [
        {
          transaction_fields.id: row[ftx_row_fields_pos.id].strip('"'),
          transaction_fields.datetime: datetime.datetime.strptime(row[ftx_row_fields_pos.time], '%d/%m/%Y, %H:%M:%S'),
          transaction_fields.currency: row[ftx_row_fields_pos.currency],
          transaction_fields.operation: "deposit",
          transaction_fields.amount: row[ftx_row_fields_pos.qta].strip('"'),
          transaction_fields.exchange: "ftx",
          transaction_fields.hash: row[ftx_row_fields_pos.trans_hash].strip('"')
        }
      ]

      writer.write_rows(rows)

if __name__ == "__main__":
  args = sys.argv[1:]
  if len(args) == 2 and args[0] == '-type':
    # Select random nice phrase
    export_type = args[0]
    print("data_import import data to {1}".format(sys.argv, export_type))
  else:
    export_type = "db"
  print("data_import import data to {1}".format(sys.argv, export_type))

  #to_cvs = True
  if export_type == "db":
    con = sqlite3.connect('trades.db')
    writer = DbWriter(con)
  else:
    csv_writer = csv.DictWriter(sys.stdout,
                            fieldnames=[transaction_fields.id,
                                        transaction_fields.datetime,
                                        transaction_fields.currency,
                                        transaction_fields.operation,
                                        transaction_fields.amount,
                                        transaction_fields.exchange,
                                        transaction_fields.hash],
                            quotechar='\"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    writer = CsvWriter(csv_writer)

  ftx_import_trades("/home/alex/Scaricati/trades.csv", writer)
  ftx_import_deposit("/home/alex/Scaricati/deposits.csv", writer)

  writer.finalize()
  pass