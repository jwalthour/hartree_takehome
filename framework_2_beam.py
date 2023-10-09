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

import argparse
import logging
import csv
import io
from typing import Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import ReadMatches

from dict_to_csv import DictToCSV
from aggregate_stats import AggregateStats

logger = logging.getLogger(__name__)


def get_csv_reader(readable_file):
    return csv.DictReader(io.TextIOWrapper(readable_file.open()))


def debug_record(record, i=None):
    """Output record to logger.debug, then return record so the pipeline can continue"""
    output = repr(record) if i is None else "%i: %r" % (i, record)
    logger.debug(output)
    return record


def unwind_joined_row(record):
    # Sample record: ('C1', {'tier': ['1'], 'invoices': [{'invoice_id': '1', 'legal_entity': 'L1', 'counter_party': 'C1', 'rating': '1', 'status': 'ARAP', 'value': '10'}, {'invoice_id': '7', 'legal_entity': 'L1', 'counter_party': 'C1', 'rating': '2', 'status': 'ARAP', 'value': '10'}, {'invoice_id': '13', 'legal_entity': 'L1', 'counter_party': 'C1', 'rating': '3', 'status': 'ARAP', 'value': '20'}]})
    # Desired output: [{'invoice_id': '1', ... 'tier': '1'}, {{'invoice_id': '7', .. 'tier': 1}]
    for invoice in record[1]["invoices"]:
        invoice["tier"] = record[1]["tier"][0]
        yield invoice


def try_parse_all_values(d: Dict) -> Dict:
    """Return a dict with the same keys, where all integer string values have been parsed to ints."""

    def try_parse_value(value: str):
        try:
            return int(value)
        except ValueError:
            return value

    return {key: try_parse_value(value) for key, value in d.items()}


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the processing pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--invoices",
        dest="invoices",
        default="data/dataset1.csv",
        help="Invoices file to process.",
    )
    parser.add_argument(
        "--tiers",
        dest="tiers",
        default="data/dataset2.csv",
        help="Tiers file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="output_beam",
        help="Output file prefix to write results to.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logger.info(
        "Will read invoices from file %s and tiers from file %s"
        % (known_args.invoices, known_args.tiers)
    )

    with beam.Pipeline(options=pipeline_options) as p:
        invoices = (
            p
            | "invoices FN" >> beam.Create([known_args.invoices])
            | "invoices match" >> ReadMatches()  # Opens files for reading
            | "invoices read" >> beam.FlatMap(get_csv_reader)
            | "invoices key on counterparty"
            >> beam.Map(lambda d: (d["counter_party"], d))
            # Sample record: ('C1', {'invoice_id': '1', 'legal_entity': 'L1', 'counter_party': 'C1', 'rating': '1', 'status': 'ARAP', 'value': '10'})
        )
        tiers = (
            p
            | "Tiers fn" >> beam.Create([known_args.tiers])
            | "Tiers match" >> ReadMatches()  # Opens files for reading
            | "tiers read" >> beam.FlatMap(get_csv_reader)
            | "tiers key on counterparty"
            >> beam.Map(lambda d: (d["counter_party"], d["tier"]))
            # Sample record: ('C5','5')
        )

        invoices_with_tiers = (
            {"tier": tiers, "invoices": invoices}
            | "Join tiers with invoices" >> beam.CoGroupByKey()
            | beam.FlatMap(unwind_joined_row)
            | beam.Map(try_parse_all_values)
            # Sample record: {'invoice_id': 6, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 6, 'status': 'ACCR', 'value': 60, 'tier': 6}
        )

        keyed_invoices = (
            invoices_with_tiers
            | "Reindex on legal entity, counter_party, tier"
            >> beam.Map(
                lambda record: (
                    (record["legal_entity"], record["counter_party"], record["tier"]),
                    record,
                )
            )
            # (('L3', 'C6', 6), {'invoice_id': 12, 'legal_entity': 'L3', 'counter_party': 'C6', 'rating': 4, 'status': 'ARAP', 'value': 80, 'tier': 6})
        )

        sentinel_value = "Total"

        cube_key_gens = [
            lambda record: record,  # unity transform - makes the pipeline simpler
            lambda record: ((record[0][0], record[0][1], sentinel_value), record[1]),
            lambda record: ((record[0][0], sentinel_value, record[0][2]), record[1]),
            lambda record: ((record[0][0], sentinel_value, sentinel_value), record[1]),
            lambda record: ((sentinel_value, sentinel_value, record[0][2]), record[1]),
            lambda record: (
                (sentinel_value, sentinel_value, sentinel_value),
                record[1],
            ),
        ]

        # Generate a new pcollection for each grouping we wish to aggregate over
        cube_keyed_invoices = [
            (
                keyed_invoices
                | "Make aggregate grouping %i" % i >> beam.Map(cube_key_gen)
                | "debug %i" % i >> beam.Map(lambda x, i=i: debug_record(x, i))
                # {'legal_entity': 'L1', 'counter_party': 'Total', 'tier': 1, 'max_rating_by_counterparty': 3, 'sum_value_arap': 40, 'sum_value_accr': 0}
            )
            for i, cube_key_gen in enumerate(cube_key_gens)
        ]

        # Compute stats on each group, and output
        field_order = [
            "legal_entity",
            "counter_party",
            "tier",
            "max_rating_by_counterparty",
            "sum_value_arap",
            "sum_value_accr",
        ]
        header_text = ",".join(field_order)
        header_pcoll = p | beam.Create([header_text])
        csv_data = (
            cube_keyed_invoices
            | beam.Flatten()
            | AggregateStats()
            | DictToCSV(field_order)
        )
        _ = (
            (header_pcoll, csv_data)
            | "flatten header onto data" >> beam.Flatten()
            | beam.io.WriteToText(known_args.output, file_name_suffix=".csv")
        )
    logger.info("Wrote output to file with prefix %s" % known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    # logging.getLogger('aggregate_stats').setLevel(logging.DEBUG)
    # logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)
    main()
