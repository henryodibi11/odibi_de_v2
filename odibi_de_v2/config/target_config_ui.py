import ipywidgets as widgets
from IPython.display import display, Javascript, clear_output
import json
import datetime

class TargetConfigUI:
    def __init__(self):
        # --- Simple Field Inputs ---
        self.simple_fields = {
            "target_id": widgets.Text(description="Target ID:", placeholder="e.g., tgt-my-output"),
            "project": widgets.Text(description="Project:", placeholder="e.g., oee"),
            "target_name": widgets.Text(description="Target Name:", placeholder="e.g., cleaned sensor output"),
            "target_type": widgets.Dropdown(description="Target Type:", options=["adls", "sql"]),
            "target_path_or_table": widgets.Text(description="Path or Table:", placeholder="/mnt/silver/data OR db.table"),
            "write_mode": widgets.Dropdown(description="Write Mode:", options=["overwrite", "append", "merge"]),
            "secret_config_id": widgets.Text(description="Secret ID:", placeholder="e.g., sec-my-secret"),
            "environment": widgets.Dropdown(description="Environment:", options=["dev", "qat", "prod"]),
            "layer": widgets.Dropdown(description="Layer:", options=["bronze", "silver", "gold"]),
        }
        self.simple_box = widgets.VBox(list(self.simple_fields.values()))

        # --- Target Options ---
        self.target_engine_dropdown = widgets.Dropdown(options=["databricks"], value="databricks",
                                                       description="Engine:", disabled=True)

        self.target_batch_inputs = {
            "bucketBy": self.build_kv_input("batch.bucketBy", "list"),
            "format": self.build_kv_input("batch.format", "string"),
            "insertInto": self.build_kv_input("batch.insertInto", "string"),
            "mode": self.build_kv_input("batch.mode", "string"),
            "options": self.build_kv_input("batch.options", "dict"),
            "partitionBy": self.build_kv_input("batch.partitionBy", "list"),
            "sortBy": self.build_kv_input("batch.sortBy", "list"),
        }

        self.target_streaming_inputs = {
            "foreach": self.build_kv_input("stream.foreach", "string"),
            "foreachBatch": self.build_kv_input("stream.foreachBatch", "string"),
            "format": self.build_kv_input("stream.format", "string"),
            "options": self.build_kv_input("stream.options", "dict"),
            "outputMode": self.build_kv_input("stream.outputMode", "string"),
            "partitionBy": self.build_kv_input("stream.partitionBy", "list"),
            "queryName": self.build_kv_input("stream.queryName", "string"),
            "trigger": self.build_kv_input("stream.trigger", "dict"),
        }

        self.target_batch_box = widgets.VBox(list(self.target_batch_inputs.values()))
        self.target_streaming_box = widgets.VBox(list(self.target_streaming_inputs.values()))

        # --- Connection Config ---
        self.connection_config_inputs = {
            "storage_unit": self.build_kv_input("storage_unit", "string"),
            "object_name": self.build_kv_input("object_name", "string"),
            "base_url": self.build_kv_input("base_url", "string"),
            "port": self.build_kv_input("port", "string"),
            "schema": self.build_kv_input("schema", "list"),
            "database": self.build_kv_input("database", "string"),
            "table": self.build_kv_input("table", "string"),
            "region": self.build_kv_input("region", "string"),
            "resource": self.build_kv_input("resource", "string"),
            "query_params": self.build_kv_input("query_params", "dict"),
            "extra_config": self.build_kv_input("extra_config", "dict"),
        }
        self.connection_box = widgets.VBox(list(self.connection_config_inputs.values()))

        # --- Merge Config ---
        self.merge_config_inputs = {
            "catalog_database": self.build_kv_input("catalog_database", "string"),
            "catalog_table": self.build_kv_input("catalog_table", "string"),
            "merge_keys": self.build_kv_input("merge_keys", "list"),
            "change_columns": self.build_kv_input("change_columns", "list"),
            "hash_column": self.build_kv_input("hash_column", "string"),
            "timestamp_column": self.build_kv_input("timestamp_column", "string"),
            "dedup_strategy": self.build_kv_input("dedup_strategy", "string"),
        }
        self.merge_box = widgets.VBox(list(self.merge_config_inputs.values()))

        # --- Buttons and Output ---
        self.output_box = widgets.Output()
        self.json_button = widgets.Button(description="Generate JSON", button_style="success")
        self.sql_button = widgets.Button(description="Generate SQL", button_style="warning")

        self.json_button.on_click(self._on_generate_json_clicked)
        self.sql_button.on_click(self._on_generate_sql_clicked)

    def render(self):
        display(widgets.HTML("<h4>Target Config Builder</h4>"))
        display(self.simple_box)

        display(widgets.HTML("<h4>Target Options</h4>"))
        display(self.target_engine_dropdown)
        display(widgets.HTML("<b>Batch</b>"))
        display(self.target_batch_box)
        display(widgets.HTML("<b>Streaming</b>"))
        display(self.target_streaming_box)

        display(widgets.HTML("<h4>Connection Config</h4>"))
        display(self.connection_box)

        display(widgets.HTML("<h4>Merge Config</h4>"))
        display(self.merge_box)

        display(widgets.HBox([self.json_button, self.sql_button]))
        display(self.output_box)

    def build_kv_input(self, label, default_type="string"):
        type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=default_type, layout=widgets.Layout(width="90px"))
        value_widget = self.build_dict_editor() if default_type == "dict" else self.create_value_input_widget(default_type)

        def on_type_change(change):
            new_widget = self.build_dict_editor() if change.new == "dict" else self.create_value_input_widget(change.new)
            row.children = [widgets.Label(label, layout=widgets.Layout(width="150px")), type_dropdown, new_widget]

        type_dropdown.observe(on_type_change, names="value")
        row = widgets.HBox([widgets.Label(label, layout=widgets.Layout(width="150px")), type_dropdown, value_widget])
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

    def get_simple_fields(self):
        return {
            k: widget.value.strip() if isinstance(widget, widgets.Textarea) else widget.value
            for k, widget in self.simple_fields.items()
        }

    def get_target_options(self):
        batch = {k: self.parse_kv_input(v) for k, v in self.target_batch_inputs.items()}
        stream = {k: self.parse_kv_input(v) for k, v in self.target_streaming_inputs.items()}
        return {
            "engine": self.target_engine_dropdown.value,
            "databricks": {
                "method": "",
                "batch": batch,
                "streaming": stream
            }
        }

    def get_connection_config(self):
        return {k: self.parse_kv_input(v) for k, v in self.connection_config_inputs.items()}

    def get_merge_config(self):
        return {k: self.parse_kv_input(v) for k, v in self.merge_config_inputs.items()}

    def get_insert_sql(self):
        simple = self.get_simple_fields()
        target_options = self.get_target_options()
        connection_config = self.get_connection_config()
        merge_config = self.get_merge_config()
        now = datetime.datetime.utcnow().isoformat()

        columns = [
            "target_id", "project", "target_name", "target_type", "target_path_or_table",
            "write_mode", "target_options", "connection_config", "secret_config_id",
            "created_at", "updated_at", "merge_config", "environment", "layer"
        ]

        values = [
            simple["target_id"],
            simple["project"],
            simple["target_name"],
            simple["target_type"],
            simple["target_path_or_table"],
            simple["write_mode"],
            json.dumps(target_options),
            json.dumps(connection_config),
            simple["secret_config_id"],
            now,
            now,
            json.dumps(merge_config),
            simple["environment"],
            simple["layer"]
        ]

        def quote(v):
            return f"'{v}'" if isinstance(v, str) else str(v)

        return f"""INSERT INTO IngestionTargetConfig (
    {', '.join(columns)}
        ) VALUES (
            {', '.join(quote(v) for v in values)}
        );"""

    def _on_generate_json_clicked(self, _):
        self.output_box.clear_output()
        with self.output_box:
            print("Simple Fields:")
            print(json.dumps(self.get_simple_fields(), indent=2))
            print("\nTarget Options:")
            print(json.dumps(self.get_target_options(), indent=2))
            print("\nConnection Config:")
            print(json.dumps(self.get_connection_config(), indent=2))
            print("\nMerge Config:")
            print(json.dumps(self.get_merge_config(), indent=2))

    def _on_generate_sql_clicked(self, _):
        self.output_box.clear_output()
        try:
            sql = self.get_insert_sql()
            with self.output_box:
                print("Generated SQL:")
                print("-" * 80)
                print(sql)
            display(Javascript(f"""
                navigator.clipboard.writeText(`{sql}`).then(() => {{
                    alert("SQL copied to clipboard!");
                }});
            """))
        except Exception as e:
            with self.output_box:
                print(f"Error generating SQL: {e}")
