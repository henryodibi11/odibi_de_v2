import ipywidgets as widgets
from IPython.display import display, Javascript, clear_output
import json
import datetime


class TransformationConfigUI:
    def __init__(self):
        self.simple_fields = {
            "transformation_id": widgets.Text(description="ID:"),
            "transformation_group_id": widgets.Text(description="Group ID:"),
            "step": widgets.IntText(description="Step:", value=1),
            "project": widgets.Text(description="Project:"),
            "layer": widgets.Dropdown(options=["bronze", "silver", "gold"], description="Layer:"),
            "environment": widgets.Dropdown(options=["qat", "prod"], description="Env:"),
            "description": widgets.Textarea(description="Description:"),
            "active": widgets.Dropdown(options=["true", "false"], description="Active:", value="true"),
            "load_type": widgets.Dropdown(options=["full", "incremental"], description="Load Type:"),
            "transformation_engine": widgets.Dropdown(options=["Dataframe", "SQL"], description="Engine:"),
            "source_table_or_view": widgets.Text(description="Source Table/View:"),
            "target_view_name": widgets.Text(description="Target View:"),
            "target_id": widgets.Text(description="Target ID:")
        }
        self.simple_box = widgets.VBox(list(self.simple_fields.values()))

        self.sql_config_inputs = {
            "columns": self.build_generic_editor("columns", "string"),
            "joins": self.build_generic_editor("joins", "string"),
            "where": self.build_generic_editor("where", "string"),
            "group_by": self.build_generic_editor("group_by", "string"),
            "having": self.build_generic_editor("having", "string"),
            "order_by": self.build_generic_editor("order_by", "string"),
            "limit": self.build_generic_editor("limit", "string"),
            "distinct": self.build_generic_editor("distinct", "string"),
            "case_when": self.build_generic_editor("case_when", "string"),
            "window_functions": self.build_generic_editor("window_functions", "string"),
            "pivot": self.build_generic_editor("pivot", "string"),
            "unpivot": self.build_generic_editor("unpivot", "string"),
            "ctes": self.build_generic_editor("ctes", "string"),
            "unions": self.build_generic_editor("unions", "string"),
            "intersects": self.build_generic_editor("intersects", "string"),
            "excepts": self.build_generic_editor("excepts", "string")
        }
        self.sql_box = widgets.VBox(list(self.sql_config_inputs.values()))

        self.framework_config_input = self.build_generic_editor("framework_transformer_config", "list")

        self.output_box = widgets.Output()
        self.generate_button = widgets.Button(description="Generate JSON", button_style="success")
        self.sql_button = widgets.Button(description="Generate SQL", button_style="warning")
        self.generate_button.on_click(self._on_generate_clicked)
        self.sql_button.on_click(self._on_generate_sql_clicked)

    def render(self):
        display(widgets.HTML("<h4>Transformation Config Builder</h4>"))
        display(self.simple_box)

        display(widgets.HTML("<h4>SQL Transformer Config</h4>"))
        display(self.sql_box)

        display(widgets.HTML("<h4>Framework Transformer Config</h4>"))
        display(self.framework_config_input)

        display(widgets.HBox([self.generate_button, self.sql_button]))
        display(self.output_box)

    def _on_generate_clicked(self, _):
        self.output_box.clear_output()
        with self.output_box:
            print("Generated Config:")
            print(json.dumps(self.get_config_dict(), indent=2))

    def _on_generate_sql_clicked(self, _):
        self.output_box.clear_output()
        sql = self.get_insert_sql()
        with self.output_box:
            print("Generated SQL Insert Statement:")
            print(sql)
        display(Javascript(f"""
        navigator.clipboard.writeText(`{sql}`).then(() => {{
            alert("SQL copied to clipboard!");
        }});
        """))

    def get_config_dict(self):
        return {
            **{k: w.value for k, w in self.simple_fields.items()},
            "sql_transformer_config": self.extract_dict_or_list(self.sql_config_inputs),
            "framework_transformer_config": self.parse_kv_input(self.framework_config_input)
        }

    def get_insert_sql(self):
        cfg = self.get_config_dict()
        columns = list(cfg.keys())
        values = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in cfg.values()]
        sql = f"INSERT INTO TransformationConfig (\n  {', '.join(columns)}\n) VALUES (\n  " + ',\n  '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in values) + "\n);"
        return sql

    def build_generic_editor(self, label, default_type="string"):
        type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=default_type, layout=widgets.Layout(width="90px"))
        value_widget = self._build_dynamic_editor(default_type)

        def on_type_change(change):
            new_widget = self._build_dynamic_editor(change.new)
            row.children = [widgets.Label(label, layout=widgets.Layout(width="150px")), type_dropdown, new_widget]

        type_dropdown.observe(on_type_change, names="value")
        row = widgets.HBox([widgets.Label(label, layout=widgets.Layout(width="150px")), type_dropdown, value_widget])
        row.extract_value = lambda: self.parse_kv_input(row)
        return row

    def _build_dynamic_editor(self, value_type):
        if value_type == "string":
            return widgets.Text()
        elif value_type == "list":
            return self._build_list_editor()
        elif value_type == "dict":
            return self._build_dict_editor()

    def _build_list_editor(self):
        items = []

        def build_row(val_type="string", nested=None):
            type_dd = widgets.Dropdown(options=["string", "dict"], value=val_type, layout=widgets.Layout(width="90px"))
            input_widget = self._build_dict_editor() if val_type == "dict" and nested else widgets.Text()
            remove_btn = widgets.Button(description="X", layout=widgets.Layout(width="30px"), button_style='danger')

            def on_type_change(change):
                new_widget = self._build_dict_editor() if change.new == "dict" else widgets.Text()
                row.children = [type_dd, new_widget, remove_btn]

            type_dd.observe(on_type_change, names="value")

            def on_remove(_):
                items.remove(row)
                container.children = items

            remove_btn.on_click(on_remove)
            row = widgets.HBox([type_dd, input_widget, remove_btn])
            return row

        def add_item(_=None):
            row = build_row()
            items.append(row)
            container.children = items

        def extract_list():
            result = []
            for row in items:
                val_type = row.children[0].value
                widget = row.children[1]
                if val_type == "string":
                    result.append(widget.value.strip())
                else:
                    result.append(widget.extract_dict())
            return result

        container = widgets.VBox()
        add_btn = widgets.Button(description="Add", button_style="info")
        add_btn.on_click(add_item)

        wrapper = widgets.VBox([container, add_btn])
        wrapper.extract_list = extract_list
        add_item()
        return wrapper

    def _build_dict_editor(self):
        kv_items = []

        def build_kv_row(key="", val_type="string"):
            key_input = widgets.Text(value=key, layout=widgets.Layout(width="150px"))
            type_dd = widgets.Dropdown(options=["string", "list", "dict"], value=val_type, layout=widgets.Layout(width="90px"))
            value_input = widgets.Text() if val_type == "string" else self._build_list_editor() if val_type == "list" else self._build_dict_editor()
            remove_btn = widgets.Button(description="X", layout=widgets.Layout(width="30px"), button_style='danger')

            def on_type_change(change):
                new_input = widgets.Text() if change.new == "string" else self._build_list_editor() if change.new == "list" else self._build_dict_editor()
                row.children = [key_input, type_dd, new_input, remove_btn]

            type_dd.observe(on_type_change, names="value")

            def on_remove(_):
                kv_items.remove(row)
                container.children = kv_items

            remove_btn.on_click(on_remove)
            row = widgets.HBox([key_input, type_dd, value_input, remove_btn])
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
                        result[key] = widget.extract_list()
                    elif val_type == "dict":
                        result[key] = widget.extract_dict()
            return result

        container = widgets.VBox()
        add_btn = widgets.Button(description="Add Field", button_style="info")
        add_btn.on_click(add_kv_row)

        wrapper = widgets.VBox([container, add_btn])
        wrapper.extract_dict = extract_dict
        add_kv_row()
        return wrapper

    def parse_kv_input(self, row):
        value_type = row.children[1].value
        widget = row.children[2]
        if value_type == "string":
            return widget.value.strip()
        elif value_type == "list":
            return widget.extract_list()
        elif value_type == "dict":
            return widget.extract_dict()

    def extract_dict_or_list(self, input_dict):
        result = {}
        for k, row in input_dict.items():
            result[k] = row.extract_value()
        return result
