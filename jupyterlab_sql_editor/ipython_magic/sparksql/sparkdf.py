import inspect
import os

from IPython import get_ipython
from IPython.display import HTML, TextDisplayObject, display
from pyspark.sql.session import SparkSession

import jupyterlab_sql_editor.ipython_magic.sparksql.spark_streaming_query as streaming
from jupyterlab_sql_editor.ipython_magic.sparksql.spark_schema_widget import (
    SparkSchemaWidget,
)
from jupyterlab_sql_editor.outputters.outputters import _display_results


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
    display_df(df, df, limit)
    return ""


def display_df(
    original_df,
    df,
    pdf,
    limit=20,
    output="grid",
    truncate=256,
    show_nonprinting=False,
    query_name=None,
    sql=None,
    streaming_mode="update",
    args=None,
):
    query = None
    start_streaming_query = df is not None and df.isStreaming and output not in ["skip", "schema", "none"]
    if start_streaming_query:
        streaming_query_name = "default_streaming_query_name"
        if query_name:
            streaming_query_name = query_name
        ctx = streaming.get_streaming_ctx(streaming_query_name, df=original_df, sql=sql, mode=streaming_mode)
        query = ctx.query
        ctx.display_streaming_query()
        display_batch_df(ctx.query_microbatch(), limit, output, truncate, show_nonprinting, args)
    else:
        display_batch_df(df, pdf, limit, output, truncate, show_nonprinting, args)
        if query_name:
            print(f"Created temporary view `{query_name}`")
            original_df.createOrReplaceTempView(query_name)
    return query


def display_batch_df(df, pdf, limit, output, truncate, show_nonprinting, args):
    """
    Execute the query unerlying the dataframe and displays ipython widgets for the schema and the result.
    """
    if output not in ["skip", "schema", "none"]:
        dataframe_name = retrieve_name(df)
        if not dataframe_name:
            dataframe_name = "schema"
        display(SparkSchemaWidget(dataframe_name, df.schema))
        display_link()

        try:
            _display_results(
                pdf[:limit],
                output=output,
                truncate=truncate,
                show_nonprinting=show_nonprinting,
                args=args,
            )
        except Exception:
            raise


def register_display():
    ip = get_ipython()
    plain_formatter = ip.display_formatter.formatters["text/plain"]
    plain_formatter.for_type_by_name("pyspark.sql.dataframe", "DataFrame", pyspark_dataframe_custom_formatter)
