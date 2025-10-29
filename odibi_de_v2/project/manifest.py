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
        Serialize to JSON.
        
        Args:
            path: Optional path to save JSON file
            indent: JSON indentation level
            
        Returns:
            JSON string representation
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
        Create a template manifest for a new project.
        
        Args:
            project_name: Name of the project
            project_type: Type of project (determines default structure)
            
        Returns:
            New ProjectManifest with sensible defaults
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
