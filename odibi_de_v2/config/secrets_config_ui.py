import ipywidgets as widgets
from IPython.display import display, clear_output, Javascript
import datetime
import json

class SecretsConfigUI:
    def __init__(self):
        self.fields = {
            "secret_config_id": widgets.Text(description="Secret ID:", placeholder="e.g., sec-azure-key"),
            "secret_scope": widgets.Text(description="Scope:", placeholder="e.g., databricks-secrets"),
            "identifier_key": widgets.Text(description="Identifier Key:", placeholder="e.g., client_id"),
            "credential_key": widgets.Text(description="Credential Key:", placeholder="e.g., client_secret"),
            "server": widgets.Text(description="Server:", placeholder="Optional hostname or IP"),
            "connection_string_key": widgets.Text(description="Conn String Key:", placeholder="Optional"),
            "auth_type": widgets.Dropdown(description="Auth Type:", options=["basic", "token", "key"]),
            "token_header_name": widgets.Text(description="Token Header:", placeholder="e.g., Authorization"),
            "description": widgets.Textarea(description="Description:", layout=widgets.Layout(width='400px', height='60px')),
        }

        self.form = widgets.VBox(list(self.fields.values()))
        self.output_box = widgets.Output()
        self.sql_button = widgets.Button(description="Generate SQL", button_style="success")
        self.sql_button.on_click(self._on_generate_sql_clicked)

    def render(self):
        display(widgets.HTML("<h4>Secrets Config Builder</h4>"))
        display(self.form)
        display(self.sql_button)
        display(self.output_box)

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

    def get_simple_fields(self):
        return {k: w.value.strip() for k, w in self.fields.items()}

    def get_insert_sql(self):
        data = self.get_simple_fields()
        now = datetime.datetime.utcnow().isoformat()

        columns = list(data.keys()) + ["created_at", "updated_at"]
        values = list(data.values()) + [now, now]

        def quote(v):
            return f"'{v}'" if isinstance(v, str) else str(v)

        return f"""INSERT INTO SecretsConfig (
    {', '.join(columns)}
        ) VALUES (
            {', '.join(quote(v) for v in values)}
        );"""
