"""
Project Scaffolding System

Automatically generates project structure and boilerplate code.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
import json
from .manifest import ProjectManifest, ProjectType


class ProjectScaffolder:
    """
    Creates complete project scaffolding with:
    - Directory structure
    - Manifest file
    - Template transformation modules
    - Config placeholders
    - Documentation
    """
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Args:
            base_path: Root directory for projects (defaults to ./projects)
        """
        self.base_path = Path(base_path or "./projects")
    
    def initialize_project(
        self,
        project_name: str,
        project_type: ProjectType = ProjectType.CUSTOM,
        create_structure: bool = True,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Create a new project with complete scaffolding.
        
        Args:
            project_name: Name of the project
            project_type: Type of project (determines structure)
            create_structure: Whether to create directory structure
            overwrite: Whether to overwrite existing project
            
        Returns:
            Dictionary with project details and created files
        """
        project_slug = project_name.lower().replace(" ", "_")
        project_path = self.base_path / project_slug
        
        # Check if project exists
        if project_path.exists() and not overwrite:
            raise ValueError(f"Project '{project_name}' already exists at {project_path}")
        
        created_files = []
        
        # Create project directory
        if create_structure:
            project_path.mkdir(parents=True, exist_ok=True)
            created_files.append(str(project_path))
        
        # Generate manifest
        manifest = ProjectManifest.create_template(project_name, project_type)
        manifest_path = project_path / "manifest.json"
        manifest.to_json(manifest_path)
        created_files.append(str(manifest_path))
        
        # Create directory structure
        if create_structure:
            dirs = self._create_directory_structure(project_path, manifest)
            created_files.extend(dirs)
        
        # Create template files
        templates = self._create_template_files(project_path, project_slug, manifest)
        created_files.extend(templates)
        
        # Create README
        readme_path = self._create_readme(project_path, project_name, manifest)
        created_files.append(str(readme_path))
        
        return {
            "project_name": project_name,
            "project_slug": project_slug,
            "project_path": str(project_path),
            "manifest_path": str(manifest_path),
            "created_files": created_files,
            "layer_order": manifest.layer_order,
            "next_steps": self._get_next_steps(project_name, project_slug)
        }
    
    def _create_directory_structure(self, project_path: Path, manifest: ProjectManifest) -> list:
        """Create project directory structure"""
        created = []
        
        # Core directories
        dirs_to_create = [
            "transformations",
            "sql",
            "sql/ddl",
            "sql/queries",
            "notebooks",
            "tests",
            "config",
            "docs",
            "metadata"
        ]
        
        # Add layer-specific directories
        for layer in manifest.layer_order:
            layer_name = layer.lower()
            dirs_to_create.extend([
                f"transformations/{layer_name}",
                f"notebooks/{layer_name}"
            ])
        
        for dir_name in dirs_to_create:
            dir_path = project_path / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Create __init__.py for Python packages
            if dir_name.startswith("transformations"):
                init_file = dir_path / "__init__.py"
                init_file.write_text('"""Transformation modules"""\n')
                created.append(str(init_file))
            
            created.append(str(dir_path))
        
        return created
    
    def _create_template_files(self, project_path: Path, project_slug: str, manifest: ProjectManifest) -> list:
        """Create template transformation files"""
        created = []
        
        # Create main transformations module
        main_module_path = project_path / "transformations" / "__init__.py"
        main_module_content = f'''"""
{manifest.project_name} Transformations

Auto-generated transformation modules for {manifest.project_name}.
"""

__version__ = "{manifest.version}"
__project__ = "{manifest.project_name}"

# Import layer-specific modules here
'''
        main_module_path.write_text(main_module_content)
        created.append(str(main_module_path))
        
        # Create template transformation for each layer
        for layer in manifest.layer_order:
            if layer.lower() == "bronze":
                continue  # Bronze is handled by ingestion
            
            layer_module = project_path / "transformations" / layer.lower() / "functions.py"
            layer_content = self._generate_layer_template(manifest.project_name, layer, manifest)
            layer_module.write_text(layer_content)
            created.append(str(layer_module))
        
        # Create example config
        config_example = project_path / "config" / "transformation_config_example.json"
        config_example.write_text(json.dumps({
            "transformation_id": "example-transformation",
            "project": project_slug,
            "environment": "qat",
            "layer": "Silver_1",
            "module": f"{project_slug}.transformations.silver_1.functions",
            "function": "example_transformation",
            "inputs": ["input_table"],
            "outputs": [{"table": "output_table", "mode": "overwrite"}],
            "constants": {}
        }, indent=2))
        created.append(str(config_example))
        
        return created
    
    def _generate_layer_template(self, project_name: str, layer: str, manifest: ProjectManifest) -> str:
        """Generate a template transformation function for a layer"""
        template = f'''"""
{layer} Layer Transformations for {project_name}

This module contains transformation functions for the {layer} layer.
Each function should be registered in the TransformationRegistry.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Optional, Dict, Any


def example_transformation(
    spark: Optional[SparkSession] = None,
    env: str = "qat",
    **kwargs
) -> DataFrame:
    """
    Example transformation function template.
    
    Args:
        spark: SparkSession (optional, will use active session if None)
        env: Environment (qat/prod)
        **kwargs: Additional parameters from transformation config
    
    Returns:
        Transformed DataFrame
    
    Example TransformationRegistry entry:
    {{
        "transformation_id": "example-{layer.lower()}",
        "project": "{project_name.lower().replace(' ', '_')}",
        "layer": "{layer}",
        "module": "{{project_slug}}.transformations.{layer.lower()}.functions",
        "function": "example_transformation",
        "inputs": ["input_table"],
        "outputs": [{{"table": "output_table", "mode": "overwrite"}}],
        "constants": {{"param1": "value1"}}
    }}
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    # Get inputs from kwargs (passed from orchestrator)
    inputs = kwargs.get('inputs', [])
    constants = kwargs.get('constants', {{}})
    outputs = kwargs.get('outputs', [])
    
    # Example transformation logic
    if inputs:
        input_table = inputs[0]
        df = spark.table(input_table)
        
        # Your transformation logic here
        transformed_df = df.select("*")  # Replace with actual logic
        
        # Save to output(s)
        if outputs:
            output_config = outputs[0]
            output_table = output_config.get('table')
            output_mode = output_config.get('mode', 'overwrite')
            
            transformed_df.write.mode(output_mode).saveAsTable(output_table)
        
        return transformed_df
    
    return spark.createDataFrame([], schema="dummy STRING")


# Add more transformation functions below
'''
        return template
    
    def _create_readme(self, project_path: Path, project_name: str, manifest: ProjectManifest) -> Path:
        """Create project README"""
        readme_path = project_path / "README.md"
        
        readme_content = f'''# {project_name}

**Project Type:** {manifest.project_type.value}  
**Version:** {manifest.version}

{manifest.description}

## 📁 Project Structure

```
{project_path.name}/
├── manifest.json              # Project configuration manifest
├── transformations/           # Transformation modules
│   ├── bronze/               # Bronze layer (ingestion)
│   ├── silver/               # Silver layer (cleansing/enrichment)
│   └── gold/                 # Gold layer (aggregations)
├── sql/                      # SQL scripts
│   ├── ddl/                  # Data definition language
│   └── queries/              # Analytical queries
├── notebooks/                # Databricks notebooks
├── tests/                    # Unit tests
├── config/                   # Configuration files
├── docs/                     # Documentation
└── metadata/                 # Metadata and schemas
```

## 🚀 Quick Start

### 1. Run the Full Pipeline

```python
from odibi_de_v2 import run_project

# Run in QAT environment
run_project(project="{project_name}", env="qat")

# Run in PROD environment
run_project(project="{project_name}", env="prod")
```

### 2. Run Specific Layers

```python
from odibi_de_v2 import run_project

# Run only Bronze ingestion
run_project(
    project="{project_name}",
    env="qat",
    target_layers=["Bronze"]
)

# Run Silver and Gold
run_project(
    project="{project_name}",
    env="qat",
    target_layers=["Silver_1", "Gold_1"]
)
```

### 3. Configure Transformations

Edit the TransformationRegistry table or use the config UI:

```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(project="{project_name}", env="qat")
ui.render()
```

## 📊 Layer Architecture

{self._format_layer_docs(manifest)}

## 🔧 Development Workflow

1. **Add new transformations:**
   - Create function in `transformations/<layer>/functions.py`
   - Register in TransformationRegistry SQL table
   - Test using unit tests in `tests/`

2. **Update configuration:**
   - Modify `manifest.json` for layer order and dependencies
   - Update TransformationRegistry for transformation logic

3. **Run tests:**
   ```bash
   pytest {project_path.name}/tests/
   ```

## 📖 Documentation

- [Architecture Overview](docs/architecture.md)
- [Transformation Guide](docs/transformations.md)
- [Testing Guide](docs/testing.md)

## 🏷️ Entity Labels

{self._format_entity_labels(manifest)}

## 👤 Ownership

**Owner:** {manifest.owner or "TBD"}  
**Tags:** {", ".join(manifest.tags) if manifest.tags else "None"}

---

*Auto-generated by odibi_de_v2 project scaffolder*
'''
        readme_path.write_text(readme_content)
        return readme_path
    
    def _format_layer_docs(self, manifest: ProjectManifest) -> str:
        """Format layer documentation section"""
        if not manifest.layers:
            return "\n".join(f"- **{layer}**" for layer in manifest.layer_order)
        
        lines = []
        for layer_name in manifest.layer_order:
            layer = manifest.layers.get(layer_name)
            if layer:
                deps = f" (depends on: {', '.join(layer.depends_on)})" if layer.depends_on else ""
                lines.append(f"- **{layer.name}**: {layer.description}{deps}")
            else:
                lines.append(f"- **{layer_name}**")
        
        return "\n".join(lines)
    
    def _format_entity_labels(self, manifest: ProjectManifest) -> str:
        """Format entity labels documentation"""
        lines = []
        for key, label in manifest.entity_labels.items():
            lines.append(f"- `{key}`: {label}")
        return "\n".join(lines)
    
    def _get_next_steps(self, project_name: str, project_slug: str) -> list:
        """Get next steps for the user"""
        return [
            f"1. Review and customize the manifest: projects/{project_slug}/manifest.json",
            f"2. Configure ingestion sources in IngestionSourceConfig table",
            f"3. Add transformation logic to transformations/ modules",
            f"4. Register transformations in TransformationRegistry table",
            f"5. Run the pipeline: run_project(project='{project_name}', env='qat')"
        ]


def initialize_project(
    project_name: str,
    project_type: str = "custom",
    base_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to initialize a new project.
    
    Args:
        project_name: Name of the project (e.g., "Customer Churn")
        project_type: Type of project (manufacturing, analytics, ml_pipeline, custom)
        base_path: Optional base path for projects
    
    Returns:
        Dictionary with project details
    
    Example:
        >>> result = initialize_project("Customer Churn", "analytics")
        >>> print(result['project_path'])
        projects/customer_churn
    """
    scaffolder = ProjectScaffolder(base_path=Path(base_path) if base_path else None)
    
    # Convert string to enum
    try:
        proj_type = ProjectType(project_type.lower())
    except ValueError:
        proj_type = ProjectType.CUSTOM
    
    result = scaffolder.initialize_project(
        project_name=project_name,
        project_type=proj_type,
        create_structure=True,
        overwrite=False
    )
    
    # Print success message
    print(f"✅ Project '{project_name}' initialized successfully!")
    print(f"📁 Location: {result['project_path']}")
    print(f"\n📋 Next steps:")
    for step in result['next_steps']:
        print(f"   {step}")
    
    return result
