"""
Utility transform for writing out a CSV file from dictionary-style row records.

Courtesy of Andrew Mo via StackOverflow, licensed CC BY-SA 3.0:
https://stackoverflow.com/a/46796204/415551
"""

from csv import DictWriter
from csv import excel
from io import StringIO
import apache_beam as beam
from apache_beam.pipeline import ParDo
from apache_beam.transforms import PTransform, DoFn

def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel):
    """ Additional properties for delimiters, escape chars, etc via an instance of csv.Dialect
        Note: This implementation does not support unicode
    """

    buf = StringIO()

    writer = DictWriter(buf,
                        fieldnames=column_order,
                        restval=missing_val,
                        extrasaction=('ignore' if discard_extras else 'raise'),
                        dialect=dialect)
    writer.writerow(element)

    return buf.getvalue().rstrip(dialect.lineterminator)


class _DictToCSVFn(DoFn):
    """ Converts a Dictionary to a CSV-formatted String

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in the input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect)

        return [result,]

class DictToCSV(PTransform):
    """ Transforms a PCollection of Dictionaries to a PCollection of CSV-formatted Strings

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in an input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def expand(self, pcoll):
        return pcoll | ParDo(_DictToCSVFn(column_order=self._column_order,
                                          missing_val=self._missing_val,
                                          discard_extras=self._discard_extras,
                                          dialect=self._dialect)
                             )