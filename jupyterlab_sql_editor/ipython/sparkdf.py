import json
import os
from time import time
from datetime import timedelta
import pyspark.sql.functions as F
from ipywidgets import Output, widgets
from IPython.display import  display, display_html, JSON, HTML
from IPython.core.display import display, HTML, clear_output, TextDisplayObject
from IPython import get_ipython

from jupyterlab_sql_editor.ipython.common import escape_control_chars, make_tag, recursive_escape, render_grid
from jupyterlab_sql_editor.ipython.SparkSchemaWidget import SparkSchemaWidget

import inspect

from pyspark.sql.session import SparkSession



def retrieve_name(var):
    top_name = None
    back_frame = inspect.currentframe().f_back
    while back_frame:
        callers_local_vars = back_frame.f_locals.items()
        for var_name, var_val in callers_local_vars:
            if var_val is var:
                #print(f"found value named: {var_name}")
                if var_name[0] != "_":
                    top_name = var_name
        back_frame =  back_frame.f_back
    return top_name


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

def display_spark_df(df, output, limit, show_nonprinting):
    displays = []
    start = time()
    if output == 'grid':
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
    elif output == 'json':
        results = df.select(F.to_json(F.struct(F.col("*"))).alias("json_str")).take(limit)
        json_array = [json.loads(r.json_str) for r in results]
        if show_nonprinting:
            recursive_escape(json_array)
        displays.append(JSON(json_array))
    elif output == 'html':
        header, contents = get_results(df, limit)
        if len(contents) > limit:
            displays.append(PlainText(data=f'Only showing top {limit} row(s)'))
        html = make_tag('tr', False,
                    ''.join(map(lambda x: make_tag('td', show_nonprinting, x, style='font-weight: bold'), header)),
                    style='border-bottom: 1px solid')
        for index, row in enumerate(contents[:limit]):
            html += make_tag('tr', False, ''.join(map(lambda x: make_tag('td', show_nonprinting, x),row)))
        displays.insert(0, HTML(make_tag('table', False, html)))
    elif output == 'text':
        text = df._jdf.showString(limit, 100, False)
        displays.append(PlainText(data=text))
    else:
        displays.append(PlainText(data=f"Invalid output option {output}, valid options are grid, json, html, text"))
    end = time()
    elapsed = end - start
    displays.append(PlainText(data=f"Execution time: {elapsed:.2f} seconds"))
    return displays

def display_link():
    link = SparkSession._instantiatedSession._sc.uiWebUrl
    appName = SparkSession._instantiatedSession._sc.appName
    applicationId = SparkSession._instantiatedSession._sc.applicationId
    reverse_proxy = os.environ.get('SPARK_UI_URL')
    if reverse_proxy:
        link = f"{reverse_proxy}/proxy/{applicationId}"
    display(HTML(f"""<a class="external" href="{link}" target="_blank" >Open Spark UI ⭐ {appName}</a>"""))

def pyspark_dataframe_custom_formatter(df, self, cycle, limit=20):
    display_df(df, limit=limit)
    return ""

def display_df(df, output="grid", limit=20, show_nonprinting=False):
    dataframe_name = retrieve_name(df)
    if not dataframe_name:
        dataframe_name = "schema"
    display(SparkSchemaWidget(dataframe_name, df.schema))
    # display any stdout/stderr in a separate output which we can later clear
    # we use this output to display the console progress bar
    out = Output()
    display(out)
    displays = []
    execution_succeded = True
    with out:
        display_link()
        try:
            displays = display_spark_df(df, output=output, limit=limit, show_nonprinting=show_nonprinting)
        except Exception as e:
            execution_succeded = False
            raise
    if execution_succeded:
        # clear any stdout/stderror that was generated
        # it can contain remanences of the console progress bar
        out.clear_output()
        for d in displays:
            display(d)

def register_display():
    ip = get_ipython()
    plain_formatter = ip.display_formatter.formatters['text/plain']
    plain_formatter.for_type_by_name('pyspark.sql.dataframe', 'DataFrame', pyspark_dataframe_custom_formatter)
