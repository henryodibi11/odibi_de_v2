import ipywidgets as widgets
from IPython.display import display, Javascript
import json

class _DynamicFormUI:
    def __init__(self, field_specs: dict):
        self.field_specs = field_specs
        self.input_widgets = {}
        self.output_box = widgets.Output()

    def render(self):
        rows = []
        for key, spec in self.field_specs.items():
            row = self.build_input_row(key, spec)
            self.input_widgets[key] = row
            rows.append(row)
        return widgets.VBox(rows)

    def build_input_row(self, key, spec):
        input_type = spec.get("type", "string")
        type_dropdown = widgets.Dropdown(
            options=["string", "list", "dict", "dropdown"],
            value=input_type,
            layout=widgets.Layout(width="100px"),
            disabled=True
        )
        input_widget = self.build_value_widget(input_type, spec)
        row = widgets.HBox([widgets.Label(key, layout=widgets.Layout(width="120px")), type_dropdown, input_widget])
        row.extract_value = lambda: self._extract_widget_value(type_dropdown.value, input_widget)
        row.set_value = lambda val: self._set_widget_value(type_dropdown.value, input_widget, val)
        return row

    def build_value_widget(self, value_type, spec=None):
        if value_type == "string":
            return widgets.Text(value="")
        elif value_type == "dropdown":
            options = spec.get("options", [])
            return widgets.Dropdown(options=options)
        elif value_type == "list":
            return self._build_list_editor()
        elif value_type == "dict":
            return self._build_dict_editor()
        return widgets.Text(value="")

    def _extract_widget_value(self, value_type, widget):
        if value_type == "string":
            return widget.value.strip()
        elif value_type == "dropdown":
            return widget.value
        elif value_type == "list":
            return widget.extract_list()
        elif value_type == "dict":
            return widget.extract_dict()

    def _set_widget_value(self, value_type, widget, val):
        if value_type == "string":
            widget.value = str(val)
        elif value_type == "dropdown":
            widget.value = val
        elif value_type == "list":
            for item in val:
                widget.add_item(item)
        elif value_type == "dict":
            for k, v in val.items():
                widget.add_kv_row(None, k, v)

    def _build_list_editor(self):
        items = []

        def build_list_item(val=None):
            value_type = "string"
            if isinstance(val, list):
                value_type = "list"
            elif isinstance(val, dict):
                value_type = "dict"

            type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=value_type, layout=widgets.Layout(width="90px"))
            value_widget = self.build_value_widget(value_type)
            remove_btn = widgets.Button(description="X", layout=widgets.Layout(width="30px"), button_style='danger')

            def on_type_change(change):
                new_widget = self.build_value_widget(change.new)
                item.children = [type_dropdown, new_widget, remove_btn]

            def on_remove(_):
                items.remove(item)
                container.children = items

            type_dropdown.observe(on_type_change, names="value")
            remove_btn.on_click(on_remove)
            item = widgets.HBox([type_dropdown, value_widget, remove_btn])
            items.append(item)
            container.children = items

            # Set value
            if value_type == "string":
                value_widget.value = str(val)
            elif value_type == "list":
                for sub in val:
                    value_widget.add_item(sub)
            elif value_type == "dict":
                for k, v in val.items():
                    value_widget.add_kv_row(None, k, v)

        def add_item(val=None):
            build_list_item(val)

        def extract_list():
            result = []
            for item in items:
                value_type = item.children[0].value
                value_widget = item.children[1]
                if value_type == "string":
                    result.append(value_widget.value.strip())
                elif value_type == "list":
                    result.append(value_widget.extract_list())
                elif value_type == "dict":
                    result.append(value_widget.extract_dict())
            return result

        container = widgets.VBox()
        add_btn = widgets.Button(description="Add Item", button_style="info")
        add_btn.on_click(lambda _: add_item())
        wrapper = widgets.VBox([container, add_btn])
        wrapper.extract_list = extract_list
        wrapper.add_item = add_item
        return wrapper

    def _build_dict_editor(self):
        kv_rows = []

        def build_kv_row(_, key="", val=None):
            value_type = "string"
            if isinstance(val, list):
                value_type = "list"
            elif isinstance(val, dict):
                value_type = "dict"

            key_input = widgets.Text(value=key, layout=widgets.Layout(width="150px"))
            type_dropdown = widgets.Dropdown(options=["string", "list", "dict"], value=value_type, layout=widgets.Layout(width="90px"))
            value_widget = self.build_value_widget(value_type)
            remove_btn = widgets.Button(description="X", layout=widgets.Layout(width="30px"), button_style='danger')

            def on_type_change(change):
                new_widget = self.build_value_widget(change.new)
                row.children = [key_input, type_dropdown, new_widget, remove_btn]

            def on_remove(_):
                kv_rows.remove(row)
                container.children = kv_rows

            type_dropdown.observe(on_type_change, names="value")
            remove_btn.on_click(on_remove)
            row = widgets.HBox([key_input, type_dropdown, value_widget, remove_btn])
            kv_rows.append(row)
            container.children = kv_rows

            # Set value
            if value_type == "string":
                value_widget.value = str(val)
            elif value_type == "list":
                for i in val:
                    value_widget.add_item(i)
            elif value_type == "dict":
                for k, v in val.items():
                    value_widget.add_kv_row(None, k, v)

        def add_kv_row(_, key="", val=None):
            build_kv_row(None, key, val)

        def extract_dict():
            result = {}
            for row in kv_rows:
                key = row.children[0].value.strip()
                value_type = row.children[1].value
                value_widget = row.children[2]
                if key:
                    if value_type == "string":
                        result[key] = value_widget.value.strip()
                    elif value_type == "list":
                        result[key] = value_widget.extract_list()
                    elif value_type == "dict":
                        result[key] = value_widget.extract_dict()
            return result

        container = widgets.VBox()
        add_btn = widgets.Button(description="Add Field", button_style="info")
        add_btn.on_click(lambda _: add_kv_row(None))
        wrapper = widgets.VBox([container, add_btn])
        wrapper.extract_dict = extract_dict
        wrapper.add_kv_row = add_kv_row
        return wrapper

    def get_form_data(self):
        return {k: w.extract_value() for k, w in self.input_widgets.items()}


class DynamicBatchFormUI:
    """
    A flexible, interactive UI for batch entry of structured configuration records using ipywidgets.

    This class renders a dynamic form that allows users to enter, edit, and remove multiple
    configuration records based on a provided field specification. It supports nested fields, 
    dropdowns, and presets for rapid form population.

    Attributes:
        field_specs (dict): A dictionary defining the schema for each field. Each key represents a 
            field name and maps to a dict with a required 'type' key and optional metadata. 
            Supported types include:
                - "string"
                - "dropdown" (requires "options" list)
                - "list" (supports nested "string", "list", "dict")
                - "dict" (supports nested types recursively)
        table_name (str): Optional name of the target table. Used for SQL generation.
        preset_records (list[dict[str, list[dict]]]): A list containing a single dictionary where keys 
            are preset names (e.g., "Steam Sensor") and values are lists of records (dicts) to pre-fill.

    Example:
        >>> field_specs = {
        ...     "Name": {"type": "string"},
        ...     "IsActive": {"type": "dropdown", "options": [0, 1]},
        ...     "Tags": {"type": "list"},  # Can contain strings or nested structures
        ...     "Extra": {"type": "dict"}  # Nested key-value fields
        ... }

        >>> preset_records = [
        ...     {
        ...         "Steam Sensor": [
        ...             {
        ...                 "Name": "Steam_001",
        ...                 "IsActive": 1,
        ...                 "Tags": [
        ...                     "flow",
        ...                     ["high", "low"],  # Nested list
        ...                     {"category": "pressure", "range": ["min", "max"]}  # Dict inside list
        ...                 ],
        ...                 "Extra": {
        ...                     "unit": "psi",
        ...                     "source": "sensor_net",
        ...                     "calibration": {
        ...                         "date": "2024-01-01",
        ...                         "method": "auto"
        ...                     }
        ...                 }
        ...             }
        ...         ]
        ...     }
        ... ]

        >>> ui = DynamicBatchFormUI(
        ...     field_specs=field_specs,
        ...     table_name="SensorConfig",
        ...     preset_records=preset_records
        ... )
        >>> ui.render()

    Methods:
        render(): Renders the full UI including buttons and row container.
        get_rows(): Returns a list of record dicts currently entered in the UI.
        _on_add_row(): Adds a new row to the form.
        _on_apply_preset(): Loads one or more preset rows into the form.
        _on_generate_json(): Prints and displays JSON-formatted output of all rows.
        _on_generate_sql(): Prints and copies SQL INSERT statements for all rows.
    """

    def __init__(self, field_specs, table_name="YourTable", preset_records=None):
        self.field_specs = field_specs
        self.table_name = table_name
        self.preset_records = preset_records or []
        self.rows = []
        self.row_container = widgets.VBox()
        self.output_box = widgets.Output()
        self.preset_dropdown = self._build_preset_dropdown()
        self._buttons = self._build_buttons()

    def render(self):
        display(widgets.HTML(f"<h4>Batch Entry for <b>{self.table_name}</b></h4>"))
        display(self._buttons)
        display(self.row_container)
        display(self.output_box)

    def _build_buttons(self):
        add_row_btn = widgets.Button(description="Add Row", button_style="info")
        generate_btn = widgets.Button(description="Generate JSON", button_style="success")
        sql_btn = widgets.Button(description="Generate SQL", button_style="warning")
        apply_preset_btn = widgets.Button(description="Apply Preset", button_style="primary")

        add_row_btn.on_click(self._on_add_row)
        generate_btn.on_click(self._on_generate_json)
        sql_btn.on_click(self._on_generate_sql)
        apply_preset_btn.on_click(self._on_apply_preset)

        return widgets.HBox([add_row_btn, apply_preset_btn, self.preset_dropdown, generate_btn, sql_btn])

    def _build_preset_dropdown(self):
        preset_names = list(self.preset_records[0].keys()) if self.preset_records else []
        return widgets.Dropdown(options=preset_names, description="Preset:")

    def _on_add_row(self, _=None, data=None):
        form = _DynamicFormUI(self.field_specs)
        row_ui = form.render()
        remove_btn = widgets.Button(description="Remove", button_style="danger")
        container = widgets.HBox([row_ui, remove_btn])

        def on_remove(_):
            self.rows.remove(container)
            self.row_container.children = self.rows

        remove_btn.on_click(on_remove)
        container.extract_data = lambda: form.get_form_data()
        self.rows.append(container)
        self.row_container.children = self.rows

        if data:
            for key, val in data.items():
                if key not in form.input_widgets:
                    continue
                form.input_widgets[key].set_value(val)

    def _on_apply_preset(self, _):
        selected = self.preset_dropdown.value
        if not selected:
            return
        preset = self.preset_records[0].get(selected, [])
        for record in preset:
            self._on_add_row(data=record)

    def _on_generate_json(self, _):
        self.output_box.clear_output()
        with self.output_box:
            data = [row.extract_data() for row in self.rows]
            print(json.dumps(data, indent=2))

    def _on_generate_sql(self, _):
        self.output_box.clear_output()
        with self.output_box:
            rows = [row.extract_data() for row in self.rows]
            sql = ""
            for r in rows:
                cols = ', '.join(r.keys())
                vals = ', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in r.values())
                sql += f"INSERT INTO {self.table_name} ({cols}) VALUES ({vals});\n"
            print(sql)
            display(Javascript(f"""
            navigator.clipboard.writeText(`{sql}`).then(() => {{
                alert("SQL copied to clipboard!");
            }});"""))
