"""
TransformationRegistry IPython UI

Interactive notebook UI for creating and editing TransformationRegistry entries.
"""

import ipywidgets as widgets
from IPython.display import display, Javascript, clear_output
import json
import datetime
from typing import Optional, Dict, Any, List


class TransformationRegistryUI:
    """
    Interactive UI for building TransformationRegistry configurations.
    
    Supports:
    - All TransformationRegistry fields
    - JSON editing for inputs, constants, outputs
    - Visual field organization
    - SQL and JSON export
    - Project and environment filtering
    
    Example:
        >>> from odibi_de_v2.config import TransformationRegistryUI
        >>> ui = TransformationRegistryUI(project="Energy Efficiency", env="qat")
        >>> ui.render()
    """
    
    def __init__(self, project: str = "", env: str = "qat"):
        """
        Initialize the UI.
        
        Args:
            project: Default project name
            env: Default environment
        """
        self.project = project
        self.env = env
        
        # Core identification fields
        self.transformation_id = widgets.Text(
            description="Transform ID:",
            value="",
            placeholder="unique-transformation-id",
            layout=widgets.Layout(width='500px')
        )
        
        self.transformation_group_id = widgets.Text(
            description="Group ID:",
            value="",
            placeholder="group-identifier",
            layout=widgets.Layout(width='500px')
        )
        
        # Project and environment
        self.project_field = widgets.Text(
            description="Project:",
            value=project,
            placeholder="Project Name",
            layout=widgets.Layout(width='500px')
        )
        
        self.environment = widgets.Dropdown(
            options=["qat", "prod", "dev"],
            value=env,
            description="Environment:"
        )
        
        # Layer and execution
        self.layer = widgets.Text(
            description="Layer:",
            value="Silver_1",
            placeholder="Bronze, Silver_1, Gold_1, etc.",
            layout=widgets.Layout(width='500px')
        )
        
        self.step = widgets.IntText(
            description="Step:",
            value=1,
            layout=widgets.Layout(width='200px')
        )
        
        self.enabled = widgets.Checkbox(
            description="Enabled",
            value=True
        )
        
        # Generic entities
        self.entity_1 = widgets.Text(
            description="Entity 1:",
            placeholder="e.g., plant, region, domain",
            layout=widgets.Layout(width='500px')
        )
        
        self.entity_2 = widgets.Text(
            description="Entity 2:",
            placeholder="e.g., asset, store, subdomain",
            layout=widgets.Layout(width='500px')
        )
        
        self.entity_3 = widgets.Text(
            description="Entity 3:",
            placeholder="e.g., equipment, department",
            layout=widgets.Layout(width='500px')
        )
        
        # Transformation logic
        self.module = widgets.Text(
            description="Module:",
            placeholder="project.transformations.silver.functions",
            layout=widgets.Layout(width='500px')
        )
        
        self.function = widgets.Text(
            description="Function:",
            placeholder="function_name",
            layout=widgets.Layout(width='500px')
        )
        
        # JSON configuration fields
        self.inputs = widgets.Textarea(
            description="Inputs (JSON):",
            value='["input_table_1"]',
            placeholder='["table1", "table2"] or [{"type": "query", "sql": "SELECT..."}]',
            layout=widgets.Layout(width='600px', height='100px')
        )
        
        self.constants = widgets.Textarea(
            description="Constants (JSON):",
            value='{}',
            placeholder='{"param1": "value1", "threshold": 100}',
            layout=widgets.Layout(width='600px', height='100px')
        )
        
        self.outputs = widgets.Textarea(
            description="Outputs (JSON):",
            value='[{"table": "output_table", "mode": "overwrite"}]',
            placeholder='[{"table": "table_name", "mode": "overwrite"}]',
            layout=widgets.Layout(width='600px', height='100px')
        )
        
        # Metadata
        self.description = widgets.Textarea(
            description="Description:",
            placeholder="Describe what this transformation does",
            layout=widgets.Layout(width='600px', height='80px')
        )
        
        # Buttons
        self.generate_json_btn = widgets.Button(
            description="Generate JSON",
            button_style="success",
            icon='check'
        )
        
        self.generate_sql_btn = widgets.Button(
            description="Generate SQL Insert",
            button_style="warning",
            icon='database'
        )
        
        self.copy_btn = widgets.Button(
            description="Copy to Clipboard",
            button_style="info",
            icon='copy'
        )
        
        self.clear_btn = widgets.Button(
            description="Clear Form",
            button_style="danger",
            icon='trash'
        )
        
        # Output area
        self.output = widgets.Output(
            layout=widgets.Layout(
                border='1px solid #ccc',
                padding='10px',
                margin='10px 0'
            )
        )
        
        # Wire up button handlers
        self.generate_json_btn.on_click(self._on_generate_json)
        self.generate_sql_btn.on_click(self._on_generate_sql)
        self.copy_btn.on_click(self._on_copy)
        self.clear_btn.on_click(self._on_clear)
    
    def render(self):
        """Render the UI"""
        display(widgets.HTML("<h2>🔧 Transformation Registry Configuration Builder</h2>"))
        
        # Section 1: Core identification
        display(widgets.HTML("<h3>📋 Identification</h3>"))
        display(widgets.VBox([
            self.transformation_id,
            self.transformation_group_id,
            self.project_field,
            self.environment
        ]))
        
        # Section 2: Execution control
        display(widgets.HTML("<h3>⚙️ Execution Control</h3>"))
        display(widgets.VBox([
            self.layer,
            self.step,
            self.enabled
        ]))
        
        # Section 3: Entity hierarchy
        display(widgets.HTML("<h3>🏷️ Entity Hierarchy (Generic)</h3>"))
        display(widgets.HTML(
            "<p style='color: #666; font-size: 0.9em;'>"
            "Entity labels are project-specific (e.g., plant/asset for manufacturing, region/store for retail)"
            "</p>"
        ))
        display(widgets.VBox([
            self.entity_1,
            self.entity_2,
            self.entity_3
        ]))
        
        # Section 4: Transformation logic
        display(widgets.HTML("<h3>🔨 Transformation Logic</h3>"))
        display(widgets.VBox([
            self.module,
            self.function
        ]))
        
        # Section 5: Data configuration
        display(widgets.HTML("<h3>📊 Data Configuration (JSON)</h3>"))
        display(widgets.VBox([
            self.inputs,
            self.constants,
            self.outputs
        ]))
        
        # Section 6: Metadata
        display(widgets.HTML("<h3>📝 Metadata</h3>"))
        display(self.description)
        
        # Section 7: Actions
        display(widgets.HTML("<h3>🚀 Actions</h3>"))
        display(widgets.HBox([
            self.generate_json_btn,
            self.generate_sql_btn,
            self.copy_btn,
            self.clear_btn
        ]))
        
        # Output area
        display(self.output)
    
    def get_config_dict(self) -> Dict[str, Any]:
        """Get current configuration as dictionary"""
        # Validate and parse JSON fields
        try:
            inputs = json.loads(self.inputs.value) if self.inputs.value.strip() else []
        except json.JSONDecodeError as e:
            inputs = f"ERROR: Invalid JSON in inputs - {e}"
        
        try:
            constants = json.loads(self.constants.value) if self.constants.value.strip() else {}
        except json.JSONDecodeError as e:
            constants = f"ERROR: Invalid JSON in constants - {e}"
        
        try:
            outputs = json.loads(self.outputs.value) if self.outputs.value.strip() else []
        except json.JSONDecodeError as e:
            outputs = f"ERROR: Invalid JSON in outputs - {e}"
        
        return {
            "transformation_id": self.transformation_id.value,
            "transformation_group_id": self.transformation_group_id.value or None,
            "project": self.project_field.value,
            "environment": self.environment.value,
            "layer": self.layer.value,
            "step": self.step.value,
            "enabled": 1 if self.enabled.value else 0,
            "entity_1": self.entity_1.value or None,
            "entity_2": self.entity_2.value or None,
            "entity_3": self.entity_3.value or None,
            "module": self.module.value,
            "function": self.function.value,
            "inputs": inputs,
            "constants": constants,
            "outputs": outputs,
            "description": self.description.value or None,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def get_sql_insert(self) -> str:
        """Generate SQL INSERT statement"""
        config = self.get_config_dict()
        
        # Check for JSON parsing errors
        errors = []
        for key in ['inputs', 'constants', 'outputs']:
            if isinstance(config[key], str) and config[key].startswith("ERROR"):
                errors.append(config[key])
        
        if errors:
            return "-- SQL GENERATION FAILED:\n-- " + "\n-- ".join(errors)
        
        # Build SQL
        columns = []
        values = []
        
        for key, value in config.items():
            if value is not None and key not in ['created_at', 'updated_at']:
                columns.append(key)
                
                if isinstance(value, (dict, list)):
                    # JSON fields
                    values.append(f"'{json.dumps(value)}'")
                elif isinstance(value, str):
                    values.append("'" + value.replace("'", "''") + "'")
                elif isinstance(value, bool):
                    values.append(str(int(value)))
                else:
                    values.append(str(value))
        
        sql = f"""INSERT INTO TransformationRegistry (
    {','.join([f"{col}" for col in columns])}
) VALUES (
    {','.join(values)}
);"""
        
        return sql
    
    def _on_generate_json(self, btn):
        """Handle JSON generation"""
        self.output.clear_output()
        with self.output:
            config = self.get_config_dict()
            print("Generated JSON Configuration:")
            print("=" * 60)
            print(json.dumps(config, indent=2, default=str))
    
    def _on_generate_sql(self, btn):
        """Handle SQL generation"""
        self.output.clear_output()
        with self.output:
            sql = self.get_sql_insert()
            print("Generated SQL INSERT Statement:")
            print("=" * 60)
            print(sql)
    
    def _on_copy(self, btn):
        """Handle copy to clipboard"""
        sql = self.get_sql_insert()
        
        # Use JavaScript to copy to clipboard
        display(Javascript(f'''
            navigator.clipboard.writeText(`{sql.replace("`", "\\`")}`).then(() => {{
                console.log("SQL copied to clipboard");
            }});
        '''))
        
        self.output.clear_output()
        with self.output:
            print("✅ SQL copied to clipboard!")
    
    def _on_clear(self, btn):
        """Clear the form"""
        self.transformation_id.value = ""
        self.transformation_group_id.value = ""
        self.project_field.value = self.project
        self.environment.value = self.env
        self.layer.value = "Silver_1"
        self.step.value = 1
        self.enabled.value = True
        self.entity_1.value = ""
        self.entity_2.value = ""
        self.entity_3.value = ""
        self.module.value = ""
        self.function.value = ""
        self.inputs.value = '["input_table_1"]'
        self.constants.value = '{}'
        self.outputs.value = '[{"table": "output_table", "mode": "overwrite"}]'
        self.description.value = ""
        
        self.output.clear_output()
        with self.output:
            print("🧹 Form cleared")


class TransformationRegistryBrowser:
    """
    Browse and edit existing TransformationRegistry entries.
    
    Requires SQL connection to read from database.
    """
    
    def __init__(self, sql_provider, project: Optional[str] = None, env: str = "qat"):
        """
        Initialize the browser.
        
        Args:
            sql_provider: SQL connection provider
            project: Optional project filter
            env: Environment filter
        """
        self.sql_provider = sql_provider
        self.project = project
        self.env = env
        
        # Load data
        self.records = self._load_records()
        
        # UI components
        self.search = widgets.Text(
            description="Search:",
            placeholder="Search by ID, project, or description",
            layout=widgets.Layout(width='500px')
        )
        
        self.filter_project = widgets.Text(
            description="Project:",
            value=project or "",
            layout=widgets.Layout(width='300px')
        )
        
        self.filter_env = widgets.Dropdown(
            options=["all", "qat", "prod"],
            value=env,
            description="Environment:"
        )
        
        self.filter_layer = widgets.Dropdown(
            options=["all"] + list(set(r['layer'] for r in self.records)),
            value="all",
            description="Layer:"
        )
        
        self.results_output = widgets.Output()
        
        # Wire up handlers
        self.search.observe(self._on_filter_change, names='value')
        self.filter_project.observe(self._on_filter_change, names='value')
        self.filter_env.observe(self._on_filter_change, names='value')
        self.filter_layer.observe(self._on_filter_change, names='value')
    
    def _load_records(self) -> List[Dict]:
        """Load records from database"""
        # Placeholder - implement based on your SQL provider
        query = f"""
        SELECT * FROM TransformationRegistry
        WHERE 1=1
        {f"AND project = '{self.project}'" if self.project else ""}
        {f"AND environment = '{self.env}'" if self.env != "all" else ""}
        ORDER BY layer, step
        """
        
        # This would use your actual SQL provider
        # df = self.sql_provider.read(query)
        # return df.to_dict('records')
        
        return []  # Placeholder
    
    def _on_filter_change(self, change):
        """Handle filter changes"""
        self._display_results()
    
    def _display_results(self):
        """Display filtered results"""
        self.results_output.clear_output()
        
        # Apply filters
        filtered = self.records
        
        if self.search.value:
            search_term = self.search.value.lower()
            filtered = [
                r for r in filtered
                if search_term in str(r.get('transformation_id', '')).lower()
                or search_term in str(r.get('project', '')).lower()
                or search_term in str(r.get('description', '')).lower()
            ]
        
        if self.filter_project.value:
            filtered = [r for r in filtered if r.get('project') == self.filter_project.value]
        
        if self.filter_env.value != "all":
            filtered = [r for r in filtered if r.get('environment') == self.filter_env.value]
        
        if self.filter_layer.value != "all":
            filtered = [r for r in filtered if r.get('layer') == self.filter_layer.value]
        
        with self.results_output:
            print(f"Found {len(filtered)} transformation(s)")
            print("=" * 80)
            
            for record in filtered[:20]:  # Limit to 20 results
                print(f"\n📋 {record.get('transformation_id')}")
                print(f"   Project: {record.get('project')} | Env: {record.get('environment')} | Layer: {record.get('layer')}")
                print(f"   Module: {record.get('module')}.{record.get('function')}")
                if record.get('description'):
                    print(f"   Description: {record.get('description')}")
    
    def render(self):
        """Render the browser UI"""
        display(widgets.HTML("<h2>🔍 Transformation Registry Browser</h2>"))
        
        display(widgets.HTML("<h3>Filters</h3>"))
        display(widgets.VBox([
            self.search,
            widgets.HBox([
                self.filter_project,
                self.filter_env,
                self.filter_layer
            ])
        ]))
        
        display(widgets.HTML("<h3>Results</h3>"))
        display(self.results_output)
        
        # Initial display
        self._display_results()
