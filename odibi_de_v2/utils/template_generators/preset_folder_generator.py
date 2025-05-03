import os
import json
from pathlib import Path


def generate_preset_package(base_dir="presets"):
    base_dir = Path(base_dir)
    os.makedirs(base_dir, exist_ok=True)

    # 1. __init__.py with working get_preset logic
    init_code = '''import importlib
import pkgutil
import os

def get_all_presets(package="presets"):
    presets = {}
    package_path = os.path.dirname(importlib.import_module(package).__file__)
    for _, module_name, is_pkg in pkgutil.iter_modules([package_path]):
        if not is_pkg:
            module = importlib.import_module(f"{package}.{module_name}")
            if hasattr(module, "FIELD_SPEC") and hasattr(module, "PRESET_RECORDS"):
                presets[module_name.lower()] = {
                    "field_spec": getattr(module, "FIELD_SPEC"),
                    "preset_records": getattr(module, "PRESET_RECORDS")
                }
    return presets

def get_preset(name: str):
    name = name.lower()
    all_presets = get_all_presets()
    if name in all_presets:
        return all_presets[name]["field_spec"], all_presets[name]["preset_records"]
    raise ValueError(f"Preset not found: {name}")
'''
    (base_dir / "__init__.py").write_text(init_code)

    # 2. example_preset.py with full type coverage
    example_code = '''FIELD_SPEC = {
    "Name": {"type": "string"},
    "IsActive": {"type": "dropdown", "options": [0, 1]},
    "Tags": {"type": "list"},
    "Extra": {"type": "dict"},
    "Nested": {"type": "dict"}
}

PRESET_RECORDS = [
    {
        "Example Config": [
            {
                "Name": "Example_001",
                "IsActive": 1,
                "Tags": ["tag1", "tag2"],
                "Extra": {"unit": "psi", "source": "sensor_net"},
                "Nested": {
                    "level1": {
                        "level2_list": ["a", "b"],
                        "level2_dict": {"x": "y"}
                    }
                }
            }
        ]
    }
]
'''
    (base_dir / "example_preset.py").write_text(example_code)

    # 3. InteractiveUI.ipynb placed in current directory
    notebook_cells = [
        {
            "cell_type": "code",
            "metadata": {},
            "source": [
                "import sys\n",
                "from pathlib import Path\n",
                "# Automatically add parent folder of `presets` to sys.path\n",
                "notebook_dir = Path.cwd()\n",
                "parent_dir = str(notebook_dir)\n",
                "if parent_dir not in sys.path:\n",
                "    sys.path.append(parent_dir)"
            ],
            "outputs": [],
            "execution_count": None
        },
        {
            "cell_type": "code",
            "metadata": {},
            "source": [
                "from presets import get_preset\n",
                "from odibi_de_v2.config import DynamicBatchFormUI\n",
                "\n",
                "field_spec, preset_records = get_preset(\"example_preset\")\n",
                "ui = DynamicBatchFormUI(\n",
                "    field_specs=field_spec,\n",
                "    table_name=\"ExampleConfig\",\n",
                "    preset_records=preset_records\n",
                ")\n",
                "ui.render()"
            ],
            "outputs": [],
            "execution_count": None
        }
    ]

    notebook_json = {
        "cells": notebook_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.x"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    with open("InteractiveUI.ipynb", "w") as f:
        json.dump(notebook_json, f, indent=2)

    print(f"✅ Preset folder and files created at: {base_dir.resolve()}")
    print("🧪 Example UI notebook created at:", Path("InteractiveUI.ipynb").resolve())
