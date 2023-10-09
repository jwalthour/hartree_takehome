"""
MIT License

Copyright (c) 2023 John Walthour

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
import apache_beam as beam

logger = logging.getLogger(__name__)


def debug_record(record, i=None):
    """Output record to logger.debug, then return record so the pipeline can continue"""
    output = repr(record) if i is None else "%i: %r" % (i, record)
    logger.debug(output)
    return record


class AggregateStats(beam.PTransform):
    """
    A transform to aggregate statistics required by the assignment.
    Expected input pcollection record format:
        (('L3', 'C6', 6), [{'invoice_id': 6, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 6, 'status': 'ACCR', 'value': 60, 'tier': 6}, {'invoice_id': 12, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 4, 'status': 'ARAP', 'value': 80, 'tier': 6}, {'invoice_id': 18, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 5, 'status': 'ARAP', 'value': 65, 'tier': 6}])
    output pcollection record: { 'legal_entity': 'L3', 'counter_party': 'C6', 'tier': 6, "max_rating_by_counterparty","sum_value_arap","sum_value_accr"}
    """

    def unwind_joined_row(record):
        # Sample record: (('L3', 'C6', 6), [{'invoice_id': 6, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 6, 'status': 'ACCR', 'value': 60, 'tier': 6}, {'invoice_id': 12, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 4, 'status': 'ARAP', 'value': 80, 'tier': 6}, {'invoice_id': 18, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 5, 'status': 'ARAP', 'value': 65, 'tier': 6}])
        # Desired output: { 'legal_entity': 'L3', 'counter_party': 'C6', 'tier': 6, "max_rating_by_counterparty","sum_value_arap","sum_value_accr"}

        return {
            "legal_entity": record[0][0],
            "counter_party": record[0][1],
            "tier": record[0][2],
            "max_rating_by_counterparty": record[1]["max_rating"][0]
            if record[1]["max_rating"]
            else 0,
            "sum_value_arap": record[1]["sum_value_arap"][0]
            if record[1]["sum_value_arap"]
            else 0,
            "sum_value_accr": record[1]["sum_value_accr"][0]
            if record[1]["sum_value_accr"]
            else 0,
        }

    def expand(self, invoices):
        # _ = invoices | beam.Map(debug_record)
        max_ratings = (
            invoices
            | beam.Map(lambda record: (record[0], record[1]["rating"]))
            | "Combine, max ratings" >> beam.CombinePerKey(max)
        )
        sum_value_arap = (
            invoices
            # Select only records with matching status
            | beam.Filter(lambda record: record[1]["status"] == "ARAP")
            # Select only the value entry from each of those records
            | beam.Map(lambda record: (record[0], record[1]["value"]))
            | "Combine, sum values where status == ARAP" >> beam.CombinePerKey(sum)
        )
        sum_value_accr = (
            invoices
            # Select only records with matching status
            | beam.Filter(lambda record: record[1]["status"] == "ACCR")
            # Select only the value entry from each of those records
            | beam.Map(lambda record: (record[0], record[1]["value"]))
            | "Combine, sum values where status == ACCR" >> beam.CombinePerKey(sum)
        )

        return (
            {
                "invoice": invoices,
                "max_rating": max_ratings,
                "sum_value_arap": sum_value_arap,
                "sum_value_accr": sum_value_accr,
            }
            | "Join aggregated columns with original data" >> beam.CoGroupByKey()
            | "Unwind joined rows" >> beam.Map(AggregateStats.unwind_joined_row)
            | beam.Map(debug_record)
        )
