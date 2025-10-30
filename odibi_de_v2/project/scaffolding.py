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
    Initialize a complete new project with scaffolding, manifest, and templates.
    
    Creates a fully-structured project directory with all necessary files, folders,
    and boilerplate code to start building a data pipeline. This is the fastest way
    to bootstrap a new odibi_de_v2 project.
    
    Args:
        project_name: Human-readable project name (e.g., "Customer Churn Analysis",
            "Energy Efficiency Dashboard"). Used in documentation and config files.
        project_type: Type of project template to create. Valid options:
            - "manufacturing": 5-layer architecture for plant/asset data
            - "analytics": Standard 3-layer medallion for analytics
            - "ml_pipeline": Optimized for machine learning workflows
            - "data_integration": Focused on ETL/ELT patterns
            - "custom": Minimal structure with generic labels
        base_path: Root directory where project folder will be created. Defaults to
            "./projects" if not specified. Will be created if it doesn't exist.
    
    Returns:
        Dictionary containing project initialization details with keys:
            - 'project_name': Original project name
            - 'project_slug': URL-safe project identifier (lowercase, underscores)
            - 'project_path': Absolute path to created project directory
            - 'manifest_path': Path to generated manifest.json
            - 'created_files': List of all created files and directories
            - 'layer_order': List of layer names from manifest
            - 'next_steps': List of recommended next actions
    
    Raises:
        ValueError: If project already exists and overwrite=False
        TypeError: If project_type is not a valid option
    
    Examples:
        **Example 1: Create Manufacturing Project**
        
            >>> from odibi_de_v2 import initialize_project
            >>> 
            >>> # Create manufacturing project with full scaffolding
            >>> result = initialize_project(
            ...     project_name="Plant Efficiency",
            ...     project_type="manufacturing"
            ... )
            >>> 
            >>> print(f"✅ Created: {result['project_path']}")
            >>> print(f"📁 Files created: {len(result['created_files'])}")
            >>> 
            >>> # Next steps are automatically printed
            >>> for step in result['next_steps']:
            ...     print(step)
        
        **Example 2: Create Analytics Project with Custom Path**
        
            >>> from odibi_de_v2 import initialize_project
            >>> 
            >>> # Create in specific directory
            >>> result = initialize_project(
            ...     project_name="Sales Dashboard",
            ...     project_type="analytics",
            ...     base_path="/data/analytics_projects"
            ... )
            >>> 
            >>> print(result['project_path'])
            /data/analytics_projects/sales_dashboard
        
        **Example 3: Create and Immediately Configure**
        
            >>> from odibi_de_v2 import initialize_project, run_project
            >>> from pathlib import Path
            >>> 
            >>> # Initialize project
            >>> result = initialize_project(
            ...     project_name="Customer 360",
            ...     project_type="analytics"
            ... )
            >>> 
            >>> # Load and customize manifest
            >>> from odibi_de_v2.project import ProjectManifest
            >>> manifest = ProjectManifest.from_json(
            ...     Path(result['manifest_path'])
            ... )
            >>> manifest.owner = "analytics-team@company.com"
            >>> manifest.tags = ["customer", "360", "analytics"]
            >>> manifest.to_json(Path(result['manifest_path']))
            >>> 
            >>> print("✅ Project configured and ready!")
        
        **Example 4: Create Custom Project with Validation**
        
            >>> from odibi_de_v2 import initialize_project
            >>> from odibi_de_v2.project import validate_manifest, ProjectManifest
            >>> from pathlib import Path
            >>> 
            >>> # Create custom project
            >>> result = initialize_project(
            ...     project_name="IoT Pipeline",
            ...     project_type="custom"
            ... )
            >>> 
            >>> # Validate generated manifest
            >>> manifest = ProjectManifest.from_json(
            ...     Path(result['manifest_path'])
            ... )
            >>> errors = validate_manifest(manifest)
            >>> 
            >>> if not errors:
            ...     print("✅ Project validated successfully!")
            ... else:
            ...     print(f"⚠️ Validation warnings: {errors}")
        
        **Example 5: Inspect Created Structure**
        
            >>> from odibi_de_v2 import initialize_project
            >>> import os
            >>> 
            >>> result = initialize_project(
            ...     project_name="Test Project",
            ...     project_type="analytics"
            ... )
            >>> 
            >>> # Examine created files
            >>> print("Created files:")
            >>> for file_path in result['created_files']:
            ...     if os.path.isfile(file_path):
            ...         print(f"  📄 {file_path}")
            ...     else:
            ...         print(f"  📁 {file_path}")
        
        **Example 6: Batch Project Creation**
        
            >>> from odibi_de_v2 import initialize_project
            >>> 
            >>> # Create multiple related projects
            >>> projects = [
            ...     ("Sales Analytics", "analytics"),
            ...     ("Inventory Management", "manufacturing"),
            ...     ("Customer Segmentation", "ml_pipeline")
            ... ]
            >>> 
            >>> created_projects = []
            >>> for name, ptype in projects:
            ...     result = initialize_project(
            ...         project_name=name,
            ...         project_type=ptype,
            ...         base_path="./multi_project_workspace"
            ...     )
            ...     created_projects.append(result)
            >>> 
            >>> print(f"Created {len(created_projects)} projects")
        
        **Example 7: Error Handling**
        
            >>> from odibi_de_v2 import initialize_project
            >>> 
            >>> try:
            ...     result = initialize_project(
            ...         project_name="My Project",
            ...         project_type="analytics"
            ...     )
            ...     print("✅ Project created successfully!")
            ... except ValueError as e:
            ...     print(f"❌ Project already exists: {e}")
            ... except Exception as e:
            ...     print(f"❌ Error: {e}")
    
    Notes:
        - Creates complete directory structure with transformations/, sql/, notebooks/,
          tests/, config/, docs/, and metadata/ folders
        - Generates manifest.json with project configuration
        - Creates template transformation functions for each layer
        - Generates __init__.py files for Python package structure
        - Creates README.md with quick start guide and documentation
        - Creates example config JSON for TransformationRegistry
        - All template files are ready to customize for your specific use case
        - Project slug (URL-safe name) is auto-generated from project_name
    
    See Also:
        - ProjectManifest.create_template: Create standalone manifest
        - ProjectScaffolder: Low-level scaffolding class
        - run_project: Execute created project
        - validate_manifest: Validate project manifest
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
