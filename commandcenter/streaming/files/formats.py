import csv
import functools
import io

import jsonlines



def jsonlines_writer(buffer: io.StringIO) -> jsonlines.Writer:
    """A writer for JSONlines streaming."""
    writer = jsonlines.Writer(buffer)
    return functools.partial(writer.write)


def csv_writer(buffer: io.StringIO) -> csv.writer:
    """A writer for CSV streaming."""
    writer = csv.writer(buffer, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    return functools.partial(writer.writerow)