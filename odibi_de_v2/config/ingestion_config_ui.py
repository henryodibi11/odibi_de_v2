from IPython.display import display, Javascript, clear_output
import ipywidgets as widgets
import json
import datetime

class IngestionConfigUI:
    def __init__(self):
        # --- Simple Field Inputs with Tooltips ---
        self.simple_fields = {
            "source_id": widgets.Text(description="Source ID:", placeholder="e.g., src-my-sensor",
                                      description_tooltip="Unique ID for this ingestion source"),
            "project": widgets.Text(description="Project:", placeholder="e.g., energy efficiency",
                                    description_tooltip="Logical project grouping"),
            "source_name": widgets.Text(description="Source Name:", placeholder="e.g., Sensor data for boiler room",
                                        description_tooltip="Human-readable source description"),
            "source_type": widgets.Dropdown(description="Source Type:", options=["adls", "api", "sql"],
                                            description_tooltip="Type of source: adls, api, or sql"),
            "source_path_or_query": widgets.Textarea(description="Path/Query:",
                                                     placeholder="e.g., /raw/data.csv OR SELECT * FROM table",
                                                     layout=widgets.Layout(width='500px', height='60px'),
                                                     description_tooltip="File path or SQL query to read data from"),
            "file_format": widgets.Dropdown(description="File Format:",
                                            options=["CSV", "JSON", "AVRO", "Delta", "API", "sql"],
                                            description_tooltip="Format of the input data"),
            "is_autoloader": widgets.Dropdown(description="Autoloader?", options=[0, 1],
                                              description_tooltip="Set to 1 if using Databricks Autoloader for streaming"),
            "secret_config_id": widgets.Text(description="Secret ID:", placeholder="e.g., sec-my-azure-key",
                                             description_tooltip="Secret reference used for auth"),
            "environment": widgets.Dropdown(description="Env:", options=["dev", "qat", "prod"],
                                            description_tooltip="Deployment environment for the pipeline"),
        }
        self.simple_box = widgets.VBox(list(self.simple_fields.values()))

        # --- Source Options ---
        self.engine_dropdown = widgets.Dropdown(options=["databricks"], value="databricks",
                                                description="Engine:", disabled=True,
                                                description_tooltip="Ingestion engine (currently fixed to databricks)")

        self.batch_inputs = {
            "options": self.build_kv_input("batch.options", "dict"),
            "format": self.build_kv_input("batch.format", "string"),
            "schema": self.build_kv_input("batch.schema", "string"),
        }

        self.streaming_inputs = {
            "options": self.build_kv_input("streaming.options", "dict"),
            "format": self.build_kv_input("streaming.format", "string"),
            "schema": self.build_kv_input("streaming.schema", "string"),
        }

        self.batch_box = widgets.VBox(list(self.batch_inputs.values()))
        self.streaming_box = widgets.VBox(list(self.streaming_inputs.values()))

        # --- Connection Config ---
        self.connection_config_inputs = {
            "storage_unit": self.build_kv_input("storage_unit", "string"),
            "object_name": self.build_kv_input("object_name", "string"),
            "base_url": self.build_kv_input("base_url", "string"),
            "port": self.build_kv_input("port", "string"),
            "database": self.build_kv_input("database", "string"),
            "table": self.build_kv_input("table", "string"),
            "query_params": self.build_kv_input("query_params", "dict"),
            "extra_config": self.build_kv_input("extra_config", "dict"),
        }
        self.connection_box = widgets.VBox(list(self.connection_config_inputs.values()))

        # Output and Buttons
        self.output_box = widgets.Output()
        self.generate_button = widgets.Button(description="Generate JSON", button_style="success")
        self.sql_button = widgets.Button(description="Generate SQL", button_style="warning")

        self.generate_button.on_click(self._on_generate_clicked)
        self.sql_button.on_click(self._on_generate_sql_clicked)

    def render(self):
        display(widgets.HTML("<h4>Ingestion Config Builder</h4>"))
        display(self.simple_box)

        display(widgets.HTML("<h4>Source Options</h4>"))
        display(widgets.HTML("<i>Configure engine + batch/streaming options. Nested keys like 'options' support dynamic key-value entry.</i>"))
        display(self.engine_dropdown)
        display(widgets.HTML("<b>Batch</b>"))
        display(self.batch_box)
        display(widgets.HTML("<b>Streaming</b>"))
        display(self.streaming_box)

        display(widgets.HTML("<h4>Connection Config</h4>"))
        display(widgets.HTML("<i>Defines how to connect to the storage or database backend.</i>"))
        display(self.connection_box)

        display(widgets.HBox([self.generate_button, self.sql_button]))
        display(self.output_box)

    def _on_generate_clicked(self, _):
        self.output_box.clear_output()
        try:
            source_options = self.get_source_options()
            connection_config = self.get_connection_config()
            simple_fields = self.get_simple_fields()
            now = datetime.datetime.utcnow().isoformat()

            with self.output_box:
                print("Simple Fields:")
                print(json.dumps(simple_fields, indent=2))
                print("\nsource_options:")
                print(json.dumps(source_options, indent=2))
                print("\nconnection_config:")
                print(json.dumps(connection_config, indent=2))
                print("\nTimestamps:")
                print(f"created_at: {now}")
                print(f"updated_at: {now}")
        except Exception as e:
            with self.output_box:
                print(f"Error: {e}")

    def _on_generate_sql_clicked(self, _):
        self.output_box.clear_output()
        try:
            sql = self.get_insert_sql()
            # Show in output box
            with self.output_box:
                print("Generated SQL Insert Statement:")
                print("-" * 80)
                print(sql)
            # Auto-copy to clipboard
            display(Javascript(f"""
            navigator.clipboard.writeText(`{sql}`).then(() => {{
                alert("SQL copied to clipboard!");
            }});
            """))
        except Exception as e:
            with self.output_box:
                print(f"Error generating SQL: {e}")

    def build_kv_input(self, label, default_type="string"):
        type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=default_type, layout=widgets.Layout(width="90px"))
        value_widget = self.build_dict_editor() if default_type == "dict" else self.create_value_input_widget(default_type)

        def on_type_change(change):
            new_widget = self.build_dict_editor() if change.new == "dict" else self.create_value_input_widget(change.new)
            row.children = [widgets.Label(label, layout=widgets.Layout(width="120px")), type_dropdown, new_widget]

        type_dropdown.observe(on_type_change, names="value")
        row = widgets.HBox([widgets.Label(label, layout=widgets.Layout(width="120px")), type_dropdown, value_widget])
        return row

    def create_value_input_widget(self, value_type):
        if value_type == "string":
            return widgets.Text(value="")
        elif value_type == "list":
            return widgets.Textarea(value="[]", placeholder='e.g., ["a", "b"]')
        elif value_type == "dict":
            return self.build_dict_editor()

    def build_dict_editor(self):
        kv_items = []

        def build_kv_row(key="", value_type="string", nested_value=None):
            key_input = widgets.Text(value=key, layout=widgets.Layout(width="150px"))
            type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=value_type, layout=widgets.Layout(width="90px"))
            value_input = nested_value if (value_type == "dict" and nested_value) else self.create_value_input_widget(value_type)
            remove_button = widgets.Button(description="X", layout=widgets.Layout(width="30px"), button_style='danger')

            def on_type_change(change):
                new_widget = self.build_dict_editor() if change.new == "dict" else self.create_value_input_widget(change.new)
                row.children = [key_input, type_dropdown, new_widget, remove_button]

            def on_remove_clicked(_):
                kv_items.remove(row)
                container.children = kv_items

            type_dropdown.observe(on_type_change, names="value")
            remove_button.on_click(on_remove_clicked)

            row = widgets.HBox([key_input, type_dropdown, value_input, remove_button])
            return row

        def add_kv_row(_=None):
            row = build_kv_row()
            kv_items.append(row)
            container.children = kv_items

        def extract_dict():
            result = {}
            for row in kv_items:
                key = row.children[0].value.strip()
                val_type = row.children[1].value
                widget = row.children[2]
                if key:
                    if val_type == "string":
                        result[key] = widget.value.strip()
                    elif val_type == "list":
                        result[key] = json.loads(widget.value.strip())
                    elif val_type == "dict":
                        result[key] = widget.extract_dict()
            return result

        container = widgets.VBox()
        add_button = widgets.Button(description="Add Field", button_style="info")
        add_button.on_click(add_kv_row)

        wrapper = widgets.VBox([container, add_button])
        wrapper.extract_dict = extract_dict
        add_kv_row()
        return wrapper

    def parse_kv_input(self, row):
        type_selected = row.children[1].value
        widget = row.children[2]
        if type_selected == "string":
            return widget.value.strip()
        elif type_selected == "list":
            return json.loads(widget.value.strip())
        elif type_selected == "dict":
            return widget.extract_dict()

    def get_source_options(self):
        batch_config = {k: self.parse_kv_input(v) for k, v in self.batch_inputs.items()}
        streaming_config = {k: self.parse_kv_input(v) for k, v in self.streaming_inputs.items()}
        return {
            "engine": self.engine_dropdown.value,
            "databricks": {
                "batch": batch_config,
                "streaming": streaming_config
            }
        }

    def get_connection_config(self):
        return {
            k: self.parse_kv_input(v)
            for k, v in self.connection_config_inputs.items()
        }

    def get_simple_fields(self):
        return {
            k: widget.value.strip() if isinstance(widget, widgets.Textarea) else widget.value
            for k, widget in self.simple_fields.items()
        }

    def get_insert_sql(self):
        simple = self.get_simple_fields()
        source_options = self.get_source_options()
        connection_config = self.get_connection_config()
        now = datetime.datetime.utcnow().isoformat()

        columns = [
            "source_id", "project", "source_name", "source_type", "source_path_or_query",
            "file_format", "is_autoloader", "source_options", "connection_config",
            "secret_config_id", "created_at", "updated_at", "environment"
        ]

        values = [
            simple["source_id"],
            simple["project"],
            simple["source_name"],
            simple["source_type"],
            simple["source_path_or_query"],
            simple["file_format"],
            simple["is_autoloader"],
            json.dumps(source_options),
            json.dumps(connection_config),
            simple["secret_config_id"],
            now,
            now,
            simple["environment"]
        ]

        def quote(v):
            return f"'{v}'" if isinstance(v, str) else str(v)

        sql = f"""INSERT INTO IngestionSourceConfig (
    {', '.join(columns)}
        ) VALUES (
            {', '.join(quote(v) for v in values)}
        );"""
        return sql

        return {
            k: self.parse_kv_input(v)
            for k, v in self.connection_config_inputs.items()
        }

    def get_simple_fields(self):
        return {
            k: widget.value.strip() if isinstance(widget, widgets.Textarea) else widget.value
            for k, widget in self.simple_fields.items()
        }
