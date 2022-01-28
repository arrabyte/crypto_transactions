# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package csvwriter
# Writer specialization to write data on csv file

from writer import Writer

class CsvWriter(Writer):
  def __init__(self, csv_writer_instance):
    self.csv_writer_instance = csv_writer_instance
    self.csv_writer_instance.writeheader()


  def write_rows(self, rows):
    self.csv_writer_instance.writerows(rows)
