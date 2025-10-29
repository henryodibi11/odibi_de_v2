"""
Project Management Module

Handles project scaffolding, manifests, and configuration.
"""

from .manifest import ProjectManifest, ProjectType, LayerConfig, validate_manifest
from .scaffolding import ProjectScaffolder, initialize_project

__all__ = [
    "ProjectManifest",
    "ProjectType",
    "LayerConfig",
    "validate_manifest",
    "ProjectScaffolder",
    "initialize_project",
]
