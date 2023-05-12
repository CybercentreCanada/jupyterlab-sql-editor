import inspect
import json
import os
from time import time

import pandas as pd
from IPython import get_ipython
from IPython.display import HTML, JSON, TextDisplayObject, display
from ipywidgets import Output
from pyspark.rdd import _load_from_socket
from pyspark.serializers import BatchedSerializer, PickleSerializer
from pyspark.sql.session import SparkSession

import jupyterlab_sql_editor.ipython.spark_streaming_query as streaming
from jupyterlab_sql_editor.ipython.common import (
    cast_unsafe_ints_to_str,
    escape_control_chars,
    recursive_escape,
    render_ag_grid,
    render_grid,
    rows_to_html,
)
from jupyterlab_sql_editor.ipython.SparkSchemaWidget import SparkSchemaWidget


def retrieve_name(var):
    """
    Walk up the call stack trying to find the name of the variable
    holding the provided dataframe instance.
    """
    top_name = None
    back_frame = inspect.currentframe().f_back
    while back_frame:
        callers_local_vars = back_frame.f_locals.items()
        for var_name, var_val in callers_local_vars:
            if var_val is var:
                # print(f"found value named: {var_name}")
                if var_name[0] != "_":
                    top_name = var_name
        back_frame = back_frame.f_back
    return top_name


class PlainText(TextDisplayObject):
    def __repr__(self):
        return self.data


def display_spark_df(df, output, limit, truncate, show_nonprinting, args):
    """
    Execute the query of the dataframe and time the execution.
    """
    displays = []
    start = time()
    has_more_data = False
    if output == "grid":
        has_more_data, pdf = to_pandas(df, limit, truncate, show_nonprinting)
        displays.append(render_grid(pdf, limit))
    elif output == "aggrid":
        has_more_data, pdf = to_pandas(df, limit, truncate, show_nonprinting)
        displays.append(render_ag_grid(pdf))
    elif output == "json":
        json_array = []
        warnings = []
        results = df.toJSON().map(lambda j: json.loads(j)).take(limit)
        if df.count() > limit:
            has_more_data = True
        # cast unsafe ints to str for display
        for row in results:
            json_array.append(cast_unsafe_ints_to_str(row, warnings))
        # add warnings to displays
        for warning in warnings:
            displays.append(warning)
        if show_nonprinting:
            recursive_escape(json_array)
        displays.append(JSON(json_array, expanded=args.expand))
    elif output == "html":
        has_more_data, html_text = to_html(df, limit, truncate, show_nonprinting)
        displays.append(HTML(html_text))
    elif output == "text":
        text = df._jdf.showString(limit, truncate, False)
        displays.append(PlainText(data=text))
    elif limit <= 0 or output == "skip" or output == "none":
        displays.append("Query execution skipped")
        return []
    elif output == "schema":
        df.printSchema()
        return []
    else:
        displays.append(PlainText(data=f"Invalid output option {output}, valid options are grid, json, html, text"))
    end = time()
    elapsed = end - start
    if has_more_data:
        message = "only showing top %d %s\n" % (
            limit,
            "row" if limit == 1 else "rows",
        )
        displays.append(PlainText(data=message))
    displays.append(PlainText(data=f"Execution time: {elapsed:.2f} seconds"))
    return displays


def display_link():
    """
    Display a link in notebook so a user can open the spark UI's details.
    """
    link = SparkSession._instantiatedSession._sc.uiWebUrl
    appName = SparkSession._instantiatedSession._sc.appName
    applicationId = SparkSession._instantiatedSession._sc.applicationId
    reverse_proxy = os.environ.get("SPARK_UI_URL")
    if reverse_proxy:
        link = f"{reverse_proxy}/proxy/{applicationId}"
    display(HTML(f"""<a class="external" href="{link}" target="_blank" >Open Spark UI ⭐ {appName}</a>"""))


def pyspark_dataframe_custom_formatter(df, self, cycle, limit=20):
    display_df(df, limit=limit)
    return ""


def display_df(
    df,
    output="grid",
    limit=20,
    truncate=512,
    show_nonprinting=False,
    query_name=None,
    sql=None,
    streaming_mode="update",
    args=None,
):
    query = None
    start_streaming_query = df.isStreaming and output not in ["skip", "schema", "none"]
    if start_streaming_query:
        streaming_query_name = "default_streaming_query_name"
        if query_name:
            streaming_query_name = query_name
        ctx = streaming.get_streaming_ctx(streaming_query_name, df=df, sql=sql, mode=streaming_mode)
        query = ctx.query
        ctx.display_streaming_query()
        display_batch_df(ctx.query_microbatch(), output, limit, truncate, show_nonprinting, args)
    else:
        display_batch_df(df, output, limit, truncate, show_nonprinting, args)
        if query_name:
            print(f"Created temporary view `{query_name}`")
            df.createOrReplaceTempView(query_name)
    return query


def display_batch_df(df, output, limit, truncate, show_nonprinting, args):
    """
    Execute the query unerlying the dataframe and displays ipython widgets for the schema and the result.
    """
    dataframe_name = retrieve_name(df)
    if not dataframe_name:
        dataframe_name = "schema"
    display(SparkSchemaWidget(dataframe_name, df.schema))
    # display any stdout/stderr in a separate output which we can later clear
    # we use this output to display the console progress bar
    out = Output()
    display(out)
    displays = []
    execution_succeeded = True
    with out:
        display_link()
        try:
            displays = display_spark_df(
                df, output=output, limit=limit, truncate=truncate, show_nonprinting=show_nonprinting, args=args
            )
        except Exception:
            execution_succeeded = False
            raise
    if execution_succeeded:
        # clear any stdout/stderror that was generated
        # it can contain remanences of the console progress bar
        out.clear_output()
        for d in displays:
            display(d)


def register_display():
    ip = get_ipython()
    plain_formatter = ip.display_formatter.formatters["text/plain"]
    plain_formatter.for_type_by_name("pyspark.sql.dataframe", "DataFrame", pyspark_dataframe_custom_formatter)


def to_html(df, max_num_rows, truncate, show_nonprinting):
    """
    Execute the query unerlying the dataframe and creates an html representation of the results.
    Code inspired from spark's dataframe.py
    """
    sock_info = df._jdf.getRowsToPython(max_num_rows, truncate)
    rows = list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))
    columns = rows[0]
    row_data = rows[1:]
    has_more_data = len(row_data) > max_num_rows
    row_data = row_data[:max_num_rows]
    html = rows_to_html(columns, row_data, show_nonprinting)
    if has_more_data:
        html += "only showing top %d %s\n" % (max_num_rows, "row" if max_num_rows == 1 else "rows")
    return has_more_data, html


def to_pandas(df, max_num_rows, truncate, show_nonprinting):
    """
    Execute the query unerlying the dataframe and creates a pandas dataframe with the results.
    Code inspired from spark's dataframe.py
    """
    sock_info = df._jdf.getRowsToPython(max_num_rows, truncate)
    rows = list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))
    head = rows[0]
    row_data = rows[1:]
    has_more_data = len(row_data) > max_num_rows
    row_data = row_data[:max_num_rows]
    pdf = pd.DataFrame(columns=head)
    for i, row in enumerate(row_data):
        if show_nonprinting:
            row = [escape_control_chars(str(v)) for v in row]
        pdf.loc[i] = row
    return has_more_data, pdf
