import os

# Bokeh for JupyterLab 4.0 not ready yet
try:
    import bokeh
    import bokeh.io
    import bokeh.plotting
    from bokeh.embed import components
except ImportError:
    pass

import dateutil.parser
import ipywidgets as widgets
from IPython.display import Javascript, display
from pyspark.sql.session import SparkSession

# global list of streaming contexts, keyed by dataframe object
context_dict = {}


def get_streaming_ctx(query_name, df=None, sql=None, mode="update"):
    bokeh.io.output_notebook(hide_banner=True)
    should_restart = False
    ctx = context_dict.get(query_name)
    # check if streaming query should be stopped
    if ctx:
        # check if streaming query is stopped
        if not ctx.is_query_running():
            print(f"query is not running, restarting streaming query {query_name}")
            should_restart = True
        else:
            if sql:
                # user provided sql statement, check if user is using the statement
                if ctx.sql != sql:
                    print(f"sql statement changed, restarting streaming query {query_name}")
                    should_restart = True
            else:
                # user provided dataframe, check if user is using the same dataframe
                if ctx.streaming_df != df:
                    print(f"different dataframe provided, restarting streaming query {query_name}")
                    should_restart = True

        if should_restart:
            ctx.stop_streaming_query()
            ctx.out.clear_output()
            ctx = None

    # check if we should create a new streaming query
    if not ctx:
        print(f"starting streaming query {query_name}")
        ctx = StreamingContext(query_name, df, sql, mode)

    context_dict[query_name] = ctx
    return ctx


class StreamingContext:
    def __init__(self, query_name, df=None, sql=None, mode=None):
        self.streaming_df = df
        self.sql = sql
        self.query_name = query_name
        self.spark = SparkSession.builder.getOrCreate()
        self.start_streaming_query(mode)

    def start_streaming_query(self, mode):
        self.query = (
            self.streaming_df.writeStream.outputMode(mode)
            .format("memory")
            .trigger(processingTime="5 seconds")
            .queryName(self.query_name)
            .start()
        )

    def open_spark_ui(self, b=None):
        sc = self.spark._sc
        link = sc.uiWebUrl
        reverse_proxy = os.environ.get("SPARK_UI_URL")
        if reverse_proxy:
            link = f"{reverse_proxy}/proxy/{sc.applicationId}"
        self.out.append_display_data(Javascript(f'window.open("{link}/StreamingQuery/");'))

    def create_spinner(self):
        file_name = os.path.join(os.path.dirname(__file__), "balls_line.gif")
        with open(file_name, "rb") as f:
            imgdata = f.read()
        self.spinner_image = widgets.Image(
            value=imgdata,
            format="gif",
            layout=widgets.Layout(min_width="220px", max_width="220px", min_heigth="20px", max_height="20px"),
        )

    def create_stop_button(self):
        self.stop_button = widgets.Button(icon="stop", button_style="warning", layout=widgets.Layout(width="180px"))
        self.stop_button.on_click(self.stop_streaming_query)

    def create_spark_ui_button(self):
        self.open_spark_ui_button = widgets.Button(
            description="Open Spark UI", icon="star-o", button_style="info", layout=widgets.Layout(width="180px")
        )
        self.open_spark_ui_button.on_click(self.open_spark_ui)

    def stop_streaming_query(self, b=None):
        self.query.stop()
        self.update_streaming_controls()

    def update_streaming_controls(self):
        if self.is_query_running():
            self.stop_button.description = "Stop"
            self.spinner_image.layout.visibility = "visible"
        else:
            self.stop_button.description = "Stopped"
            self.spinner_image.layout.visibility = "hidden"

    def query_microbatch(self):
        microbatch_results = self.spark.sql(f"SELECT * FROM {self.query_name}")
        return microbatch_results

    def is_query_running(self):
        for q in self.spark.streams.active:
            if self.query_name == q.name:
                return True
        return False

    def display_streaming_query(self):
        # creating a brand new output, somehow clear_output did not get rid of the JavaScript code
        # causing display_streaming_query() to execute the JavaScript window.open we use to open
        # the spark ui
        self.out = widgets.Output()
        self.display_controls()
        self.display_streaming_metrics()
        display(self.out)

    def display_controls(self):
        self.create_spinner()
        self.create_stop_button()
        self.create_spark_ui_button()
        box = widgets.HBox([self.spinner_image, self.stop_button, self.open_spark_ui_button])
        box.layout.align_items = "center"
        self.update_streaming_controls()
        self.out.append_display_data(box)

    # duration values are not always present in the recent progress
    def get_duration_value(self, duration, name):
        value = duration.get(name)
        if not value:
            value = 0
        return value

    def sum_duration(self, duration):
        return (
            self.get_duration_value(duration, "addBatch")
            + self.get_duration_value(duration, "getBatch")
            + self.get_duration_value(duration, "latestOffset")
            + self.get_duration_value(duration, "queryPlanning")
            + self.get_duration_value(duration, "walCommit")
        )

    def display_streaming_metrics(self):
        timestamp = []
        processedRate = []
        inputRate = []
        duration = []

        avgProcessed = 0
        avgInput = 0
        avgDuration = 0

        if len(self.query.recentProgress) > 0:
            timestamp = list(map(lambda p: dateutil.parser.isoparse(p["timestamp"]), self.query.recentProgress))
            processedRate = list(map(lambda p: p["processedRowsPerSecond"], self.query.recentProgress))
            inputRate = list(map(lambda p: p["inputRowsPerSecond"], self.query.recentProgress))
            duration = list(map(lambda p: self.sum_duration(p["durationMs"]), self.query.recentProgress))

            avgProcessed = sum(processedRate) / len(processedRate)
            avgInput = sum(inputRate) / len(inputRate)
            avgDuration = sum(duration) / len(duration)

        # create a new plot with a title and axis labels
        p1 = bokeh.plotting.figure(
            width=400,
            height=250,
            background_fill_color="#fafafa",
            toolbar_location=None,
            title=f"Input vs Processing Rate (avg {avgInput:.0f} row/s vs {avgProcessed:.0f} row/s)",
            x_axis_type="datetime",
            x_axis_label="time",
            y_axis_label="row / s",
        )
        # add a line renderer with legend and line thickness to the plot
        p1.line(timestamp, inputRate, color="red", legend_label="input", line_width=2)
        p1.line(timestamp, processedRate, legend_label="processing", line_width=2)
        p1.legend.location = "top_left"

        # create a new plot with a title and axis labels
        p2 = bokeh.plotting.figure(
            width=400,
            height=250,
            background_fill_color="#fafafa",
            toolbar_location=None,
            title=f"Batch execution time (avg {avgDuration:.0f} ms)",
            x_axis_type="datetime",
            x_axis_label="time",
            y_axis_label="time (ms)",
        )
        # add a line renderer with legend and line thickness to the plot
        p2.line(timestamp, duration, line_width=2)

        # instead of using bokeh show() function we use components()
        # thus we get the bokeh chart's java script code and the html div
        script, div = components((p1, p2))
        chart1 = widgets.HTML(div[0])
        chart2 = widgets.HTML(div[1])
        hbox = widgets.HBox([chart1, chart2])
        accordion = widgets.Accordion(children=[hbox], selected_index=None)

        status = self.query.status["message"]
        available = "No data"
        if self.query.status["isDataAvailable"]:
            available = "Data available"
        active = "Trigger inactive"
        if self.query.status["isTriggerActive"]:
            active = "Trigger active"

        title = f"{status} / {available} / {active} / id: {self.query.id}"
        accordion.set_title(0, title)
        display(accordion)
        script = script.replace('<script type="text/javascript">', "").replace("</script>", "")
        display(Javascript(script))
