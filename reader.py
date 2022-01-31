# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package docstring
# Reader is a base class to read transactions data in a specific exchange format.

import csv

class Reader:
  # rows return a generator that allows to iterate rows
  def rows(self):
    pass
  def finalize(self):
    pass

class CsvReader(Reader):

  def __init__(self, filename, skip_header=False):
    self.filename = filename
    self.skip_header = skip_header

  def rows(self):
    with open(self.filename, newline='') as csvfile:
      self.spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')

      if self.skip_header:
        next(self.spamreader)

      for row in self.spamreader:
        yield row

  def finalize(self):
    pass