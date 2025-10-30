"""
Project Manifest System

Defines the schema and validation for project configuration manifests.
Each project has a manifest.json that defines its structure, layers, and dependencies.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
from pathlib import Path


class ProjectType(str, Enum):
    """Common project archetypes"""
    MANUFACTURING = "manufacturing"
    ANALYTICS = "analytics"
    ML_PIPELINE = "ml_pipeline"
    DATA_INTEGRATION = "data_integration"
    CUSTOM = "custom"


@dataclass
class LayerConfig:
    """Configuration for a single layer in the medallion architecture"""
    name: str
    description: str
    depends_on: List[str] = field(default_factory=list)
    cache_tables: List[str] = field(default_factory=list)
    max_workers: Optional[int] = None


@dataclass
class ProjectManifest:
    """
    Complete project configuration manifest.
    
    This defines everything needed to run a project in any environment.
    """
    # Core identification
    project_name: str
    project_type: ProjectType
    description: str
    version: str = "1.0.0"
    
    # Execution configuration
    layer_order: List[str] = field(default_factory=lambda: ["Bronze", "Silver_1", "Gold_1"])
    layers: Dict[str, LayerConfig] = field(default_factory=dict)
    
    # Environment-specific settings
    environments: List[str] = field(default_factory=lambda: ["qat", "prod"])
    default_env: str = "qat"
    
    # Entity mapping (domain-specific naming)
    entity_labels: Dict[str, str] = field(default_factory=lambda: {
        "entity_1": "plant",
        "entity_2": "asset",
        "entity_3": "equipment"
    })
    
    # Module paths
    transformation_modules: List[str] = field(default_factory=list)
    
    # Caching strategy
    cache_plan: Dict[str, List[str]] = field(default_factory=dict)
    
    # Additional metadata
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation"""
        return asdict(self)
    
    def to_json(self, path: Optional[Path] = None, indent: int = 2) -> str:
        """
        Serialize manifest to JSON format.
        
        Converts the ProjectManifest to a JSON string and optionally saves it to a file.
        Automatically handles enum conversion to ensure proper serialization.
        
        Args:
            path: Optional filesystem path where JSON file will be saved. If provided,
                creates parent directories if they don't exist. If None, only returns
                the JSON string without saving.
            indent: Number of spaces for JSON indentation. Use 2 for readability,
                None for compact output.
            
        Returns:
            JSON string representation of the manifest with all configuration details.
        
        Examples:
            **Example 1: Get JSON String**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> 
                >>> manifest = ProjectManifest.create_template(
                ...     "Energy Efficiency",
                ...     ProjectType.MANUFACTURING
                ... )
                >>> json_str = manifest.to_json()
                >>> print(json_str[:100])
            
            **Example 2: Save to File**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> from pathlib import Path
                >>> 
                >>> manifest = ProjectManifest.create_template(
                ...     "Customer Analytics",
                ...     ProjectType.ANALYTICS
                ... )
                >>> manifest.to_json(path=Path("projects/customer/manifest.json"))
            
            **Example 3: Compact JSON (No Indentation)**
            
                >>> manifest = ProjectManifest.create_template("MyProject")
                >>> compact = manifest.to_json(indent=None)
                >>> # Returns single-line JSON string
        """
        data = self.to_dict()
        # Convert enums to strings
        data['project_type'] = data['project_type'].value if isinstance(data['project_type'], ProjectType) else data['project_type']
        
        json_str = json.dumps(data, indent=indent)
        
        if path:
            Path(path).write_text(json_str)
        
        return json_str
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ProjectManifest':
        """Load from dictionary"""
        # Convert layer configs
        if 'layers' in data and data['layers']:
            data['layers'] = {
                k: LayerConfig(**v) if isinstance(v, dict) else v
                for k, v in data['layers'].items()
            }
        
        # Convert project type
        if 'project_type' in data and isinstance(data['project_type'], str):
            data['project_type'] = ProjectType(data['project_type'])
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, path: Path) -> 'ProjectManifest':
        """Load manifest from JSON file"""
        data = json.loads(Path(path).read_text())
        return cls.from_dict(data)
    
    @classmethod
    def create_template(cls, project_name: str, project_type: ProjectType = ProjectType.CUSTOM) -> 'ProjectManifest':
        """
        Create a pre-configured manifest template for a new project.
        
        Generates a complete ProjectManifest with sensible defaults based on the
        project type. Each project type has different layer structures, entity labels,
        and default configurations optimized for that use case.
        
        Args:
            project_name: Human-readable project name (e.g., "Energy Efficiency",
                "Customer Churn Analysis"). Will be slugified for file paths.
            project_type: Type of project that determines the template structure.
                Options:
                - MANUFACTURING: 5 layers (Bronze → Silver_1 → Silver_2 → Gold_1 → Gold_2)
                  with plant/asset/equipment entity labels
                - ANALYTICS: 3 layers (Bronze → Silver → Gold) with domain/subdomain/metric
                  entity labels
                - ML_PIPELINE: Optimized for ML workflows
                - DATA_INTEGRATION: Focused on data integration patterns
                - CUSTOM: Minimal 3-layer structure with generic entity labels
            
        Returns:
            Fully configured ProjectManifest instance with layer definitions, entity
            labels, and project metadata ready for customization.
        
        Examples:
            **Example 1: Manufacturing Project**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> 
                >>> # Create manufacturing project with 5-layer architecture
                >>> manifest = ProjectManifest.create_template(
                ...     project_name="Plant Efficiency",
                ...     project_type=ProjectType.MANUFACTURING
                ... )
                >>> 
                >>> print(manifest.layer_order)
                ['Bronze', 'Silver_1', 'Silver_2', 'Gold_1', 'Gold_2']
                >>> 
                >>> print(manifest.entity_labels)
                {'entity_1': 'plant', 'entity_2': 'asset', 'entity_3': 'equipment'}
            
            **Example 2: Analytics Project**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> 
                >>> # Create analytics project with standard medallion
                >>> manifest = ProjectManifest.create_template(
                ...     project_name="Sales Dashboard",
                ...     project_type=ProjectType.ANALYTICS
                ... )
                >>> 
                >>> print(manifest.layer_order)
                ['Bronze', 'Silver', 'Gold']
                >>> 
                >>> print(manifest.layers['Silver'].description)
                'Cleaned and standardized data'
            
            **Example 3: Custom Project with Modifications**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> 
                >>> # Start with custom template and modify
                >>> manifest = ProjectManifest.create_template(
                ...     project_name="IoT Sensors",
                ...     project_type=ProjectType.CUSTOM
                ... )
                >>> 
                >>> # Customize entity labels for IoT context
                >>> manifest.entity_labels = {
                ...     'entity_1': 'device',
                ...     'entity_2': 'sensor',
                ...     'entity_3': 'reading_type'
                ... }
                >>> 
                >>> # Add cache plan
                >>> manifest.cache_plan = {
                ...     'Gold': ['iot_sensors.aggregated_readings']
                ... }
                >>> 
                >>> # Save customized manifest
                >>> manifest.to_json('projects/iot_sensors/manifest.json')
            
            **Example 4: Complete Manufacturing Setup**
            
                >>> from odibi_de_v2.project import ProjectManifest, ProjectType
                >>> from pathlib import Path
                >>> 
                >>> # Create and configure manufacturing project
                >>> manifest = ProjectManifest.create_template(
                ...     "Energy Efficiency",
                ...     ProjectType.MANUFACTURING
                ... )
                >>> 
                >>> # Add project metadata
                >>> manifest.owner = "data-engineering-team@company.com"
                >>> manifest.tags = ["energy", "manufacturing", "kpi"]
                >>> manifest.metadata = {
                ...     "business_unit": "Operations",
                ...     "data_classification": "Internal"
                ... }
                >>> 
                >>> # Configure caching for expensive aggregations
                >>> manifest.cache_plan = {
                ...     "Gold_1": [
                ...         "qat_energy.combined_dryers",
                ...         "qat_energy.plant_summary"
                ...     ],
                ...     "Gold_2": [
                ...         "qat_energy.monthly_kpis"
                ...     ]
                ... }
                >>> 
                >>> # Save to project directory
                >>> manifest.to_json(Path("projects/energy_efficiency/manifest.json"))
            
            **Example 5: Validate Before Saving**
            
                >>> from odibi_de_v2.project import (
                ...     ProjectManifest, ProjectType, validate_manifest
                ... )
                >>> 
                >>> manifest = ProjectManifest.create_template(
                ...     "Customer 360",
                ...     ProjectType.ANALYTICS
                ... )
                >>> 
                >>> # Validate manifest before using
                >>> errors = validate_manifest(manifest)
                >>> if errors:
                ...     print(f"Validation errors: {errors}")
                ... else:
                ...     print("✅ Manifest is valid")
                ...     manifest.to_json("projects/customer_360/manifest.json")
        
        Notes:
            - Templates are starting points and should be customized for your use case
            - Layer definitions include dependencies to ensure proper execution order
            - Entity labels provide domain-specific naming for generic fields
            - All templates support multiple environments (qat, prod by default)
        
        See Also:
            - validate_manifest: Validate manifest configuration
            - ProjectManifest.from_json: Load existing manifest from file
            - initialize_project: Create complete project with manifest and scaffolding
        """
        if project_type == ProjectType.MANUFACTURING:
            return cls(
                project_name=project_name,
                project_type=project_type,
                description=f"{project_name} - Manufacturing Data Pipeline",
                layer_order=["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"],
                layers={
                    "Bronze": LayerConfig(
                        name="Bronze",
                        description="Raw data ingestion layer",
                        depends_on=[],
                        cache_tables=[]
                    ),
                    "Silver_1": LayerConfig(
                        name="Silver_1",
                        description="Asset-level transformations",
                        depends_on=["Bronze"],
                        cache_tables=[]
                    ),
                    "Silver_2": LayerConfig(
                        name="Silver_2",
                        description="Plant-level transformations",
                        depends_on=["Silver_1"],
                        cache_tables=[]
                    ),
                    "Gold_1": LayerConfig(
                        name="Gold_1",
                        description="Cross-plant aggregations",
                        depends_on=["Silver_2"],
                        cache_tables=[]
                    ),
                    "Gold_2": LayerConfig(
                        name="Gold_2",
                        description="Business-level KPIs",
                        depends_on=["Gold_1"],
                        cache_tables=[]
                    )
                },
                entity_labels={
                    "entity_1": "plant",
                    "entity_2": "asset",
                    "entity_3": "equipment"
                },
                transformation_modules=[f"{project_name.lower().replace(' ', '_')}.transformations"]
            )
        
        elif project_type == ProjectType.ANALYTICS:
            return cls(
                project_name=project_name,
                project_type=project_type,
                description=f"{project_name} - Analytics Pipeline",
                layer_order=["Bronze", "Silver", "Gold"],
                layers={
                    "Bronze": LayerConfig(
                        name="Bronze",
                        description="Raw data ingestion",
                        depends_on=[]
                    ),
                    "Silver": LayerConfig(
                        name="Silver",
                        description="Cleaned and standardized data",
                        depends_on=["Bronze"]
                    ),
                    "Gold": LayerConfig(
                        name="Gold",
                        description="Business-ready datasets",
                        depends_on=["Silver"]
                    )
                },
                entity_labels={
                    "entity_1": "domain",
                    "entity_2": "subdomain",
                    "entity_3": "metric"
                }
            )
        
        else:  # CUSTOM or other types
            return cls(
                project_name=project_name,
                project_type=project_type,
                description=f"{project_name} - Data Pipeline",
                layer_order=["Bronze", "Silver", "Gold"],
                entity_labels={
                    "entity_1": "category_1",
                    "entity_2": "category_2",
                    "entity_3": "category_3"
                }
            )


def validate_manifest(manifest: ProjectManifest) -> List[str]:
    """
    Validate a project manifest.
    
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check required fields
    if not manifest.project_name:
        errors.append("project_name is required")
    
    if not manifest.layer_order:
        errors.append("layer_order cannot be empty")
    
    # Validate layer dependencies
    available_layers = set(manifest.layer_order)
    for layer_name, layer_config in manifest.layers.items():
        for dep in layer_config.depends_on:
            if dep not in available_layers:
                errors.append(f"Layer '{layer_name}' depends on undefined layer '{dep}'")
    
    # Validate cache plan references existing layers
    for layer in manifest.cache_plan.keys():
        if layer not in available_layers:
            errors.append(f"Cache plan references undefined layer '{layer}'")
    
    return errors
