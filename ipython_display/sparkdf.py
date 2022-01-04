import json
import os
from time import time
from ipython_display.common import make_tag, recursive_escape, render_grid
from IPython.core.display import display, HTML, JSON, clear_output, TextDisplayObject
from datetime import timedelta
import pyspark.sql.functions as F


class PlainText(TextDisplayObject):
    def __repr__(self):
        return self.data


def get_results(df, limit):
    def convert_value(value):
        if value is None:
            return 'null'
        return str(value)

    header = df.columns
    contents = list(map(lambda row: list(map(convert_value, row)), df.take(limit + 1)))
    return header, contents


def display_spark_df(df, display_type, limit, show_nonprinting):
    displays = []
    start = time()
    if display_type == 'grid':
        pdf = df.limit(limit + 1).toPandas()
        if show_nonprinting:
            for column in pdf.columns:
                pdf[column] = pdf[column].apply(lambda v: escape_control_chars(str(v)))
        num_rows = pdf.shape[0]
        if num_rows > limit:
            displays.append(PlainText(data=f'Only showing top {limit} row(s)'))
            # Delete last row
            pdf = pdf.head(num_rows -1)
        displays.insert(0, render_grid(pdf, limit))
    elif display_type == 'json':
        results = df.select(F.to_json(F.struct(F.col("*"))).alias("json_str")).take(limit)
        json_array = [json.loads(r.json_str) for r in results]
        if show_nonprinting:
            recursive_escape(json_array)
        displays.append(JSON(json_array))
    elif display_type == 'html':
        header, contents = get_results(df, limit)
        if len(contents) > limit:
            displays.append(PlainText(data=f'Only showing top {limit} row(s)'))
        html = make_tag('tr', False,
                    ''.join(map(lambda x: make_tag('td', show_nonprinting, x, style='font-weight: bold'), header)),
                    style='border-bottom: 1px solid')
        for index, row in enumerate(contents[:limit]):
            html += make_tag('tr', False, ''.join(map(lambda x: make_tag('td', show_nonprinting, x),row)))
        displays.insert(0, HTML(make_tag('table', False, html)))
    elif display_type == 'text':
        text = df._jdf.showString(limit, 100, False)
        displays.append(PlainText(data=text))
    end = time()
    elapsed = end - start
    displays.append(PlainText(data="Execution time: " + str(timedelta(seconds=elapsed))))
    return displays
