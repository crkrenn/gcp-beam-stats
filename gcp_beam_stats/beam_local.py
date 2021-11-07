#!/usr/bin/env python3

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.textio import ReadAllFromText, WriteToText
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import BytesCoder
from apache_beam.transforms.util import WithKeys


def run_pipeline():
    p = beam.Pipeline(options=PipelineOptions())
    numbers = p | beam.Create(["numbers.txt"])

    (numbers
        | 'Read' >> ReadAllFromText(skip_header_lines=1)
        | 'Add keys' >> WithKeys("num")
        | 'write' >> WriteToText(
            'numbers_out.txt')
    )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run_pipeline()
