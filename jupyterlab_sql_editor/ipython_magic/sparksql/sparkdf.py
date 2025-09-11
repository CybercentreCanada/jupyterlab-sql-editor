import inspect
import os
from argparse import Namespace

import pandas as pd
from IPython.display import HTML, display
from pyspark.sql.dataframe import DataFrame
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
    back_frame = None

    current_frame = inspect.currentframe()
    if current_frame:
        back_frame = current_frame.f_back

    while back_frame:
        callers_local_vars = back_frame.f_locals.items()
        for var_name, var_val in callers_local_vars:
            if var_val is var:
                if var_name[0] != "_":
                    top_name = var_name
        back_frame = back_frame.f_back
    return top_name


def display_link():
    """
    Display a link in notebook so a user can open the spark UI's details.
    """
    instantiated_session = SparkSession._instantiatedSession
    if instantiated_session:
        link = instantiated_session._sc.uiWebUrl
        appName = instantiated_session._sc.appName
        applicationId = instantiated_session._sc.applicationId
        reverse_proxy = os.environ.get("SPARK_UI_URL")
        if reverse_proxy:
            link = f"{reverse_proxy}/proxy/{applicationId}"
        display(HTML(f"""<a class="external" href="{link}" target="_blank" >Open Spark UI ⭐ {appName}</a>"""))


def display_df(
    original_df: DataFrame,
    df: DataFrame | None,
    pdf: pd.DataFrame = pd.DataFrame([]),
    result_id: str = "",
    limit: int = 20,
    output: str = "grid",
    truncate: int = 256,
    show_nonprinting: bool = False,
    query_name: str = "",
    sql: str = "",
    streaming_mode: str = "update",
    args: Namespace = Namespace(),
):
    query = None
    start_streaming_query = df is not None and df.isStreaming and output not in ["skip", "schema", "none"]
    if start_streaming_query:
        streaming_query_name = query_name or "default_streaming_query_name"
        ctx = streaming.get_streaming_ctx(streaming_query_name, df=original_df, sql=sql, mode=streaming_mode)
        query = ctx.query
        ctx.display_streaming_query()
        display_batch_df(ctx.query_microbatch(), pdf, result_id, limit, output, truncate, show_nonprinting, args)
    else:
        if df is not None:
            display_batch_df(df, pdf, result_id, limit, output, truncate, show_nonprinting, args)
        if query_name:
            print(f"Created temporary view `{query_name}`")
            original_df.createOrReplaceTempView(query_name)
    return query


def display_batch_df(
    df: DataFrame,
    pdf: pd.DataFrame,
    result_id: str,
    limit: int,
    output: str,
    truncate: int,
    show_nonprinting: bool,
    args: Namespace,
):
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
                pdf=pdf.head(limit),
                output=output,
                result_id=result_id,
                truncate=truncate,
                show_nonprinting=show_nonprinting,
                args=args,
            )
        except Exception:
            raise
