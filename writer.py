# Copyright (C)2022 - Alessandro Arrabito - <arrabitoster@gmail.com>

## @package docstring
# Writer is a base class to write transactions data in a standard format.

from collections import namedtuple

## fields of imported data from various exchanges
TransFields = namedtuple("TransFields", "id datetime currency operation amount exchange hash")
transaction_fields = TransFields("id", "datetime", "currency", "operation", "amount", "exchange", "hash")

class Writer:
  def write_rows(self):
    pass

  def finalize(self):
    pass
