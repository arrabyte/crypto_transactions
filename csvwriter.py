# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package csvwriter
# Writer specialization to write data on csv file

import sys
from writer import Writer
from writer import transaction_fields
import csv

class CsvWriter(Writer):
  def __init__(self):
    self.csv_writer_instance = csv.DictWriter(sys.stdout,
                        fieldnames=[transaction_fields.id,
                                    transaction_fields.datetime,
                                    transaction_fields.currency,
                                    transaction_fields.operation,
                                    transaction_fields.amount,
                                    transaction_fields.exchange,
                                    transaction_fields.hash],
                        quotechar='\"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')

    self.csv_writer_instance.writeheader()


  def write_rows(self, rows):
    self.csv_writer_instance.writerows(rows)
