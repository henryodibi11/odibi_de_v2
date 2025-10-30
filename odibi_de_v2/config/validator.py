"""
Configuration Validator

Validates TransformationRegistry entries and project configurations.
Checks JSON fields, module paths, and common misconfigurations.
"""

from typing import List, Dict, Any, Optional
import json
import importlib.util
from pathlib import Path
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of validation check"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    
    def __str__(self):
        if self.is_valid:
            return "✅ Validation passed"
        return f"❌ {len(self.errors)} error(s), {len(self.warnings)} warning(s)"


class ConfigValidator:
    """
    Validates TransformationRegistry configurations.
    
    Checks for:
    - Valid JSON in inputs, constants, outputs
    - Required fields present
    - Module paths exist
    - Function names are valid Python identifiers
    - Input/output table references
    - Common misconfigurations
    
    Examples:
        **Example 1: Validate Single Config**
        
            >>> from odibi_de_v2.config import ConfigValidator
            >>> 
            >>> config = {
            ...     "transformation_id": "my-transform",
            ...     "project": "MyProject",
            ...     "layer": "Silver_1",
            ...     "module": "myproject.transformations.silver",
            ...     "function": "process_data",
            ...     "inputs": '["input_table"]',
            ...     "constants": '{"threshold": 100}',
            ...     "outputs": '[{"table": "output", "mode": "overwrite"}]'
            ... }
            >>> 
            >>> validator = ConfigValidator()
            >>> result = validator.validate_config(config)
            >>> print(result)
        
        **Example 2: Validate Multiple Configs**
        
            >>> from odibi_de_v2.config import ConfigValidator
            >>> 
            >>> configs = [config1, config2, config3]
            >>> validator = ConfigValidator()
            >>> results = validator.validate_configs(configs)
            >>> 
            >>> for cfg, result in zip(configs, results):
            ...     if not result.is_valid:
            ...         print(f"❌ {cfg['transformation_id']}: {result.errors}")
        
        **Example 3: Check JSON Fields Only**
        
            >>> from odibi_de_v2.config import ConfigValidator
            >>> 
            >>> validator = ConfigValidator()
            >>> result = validator.validate_json_fields({
            ...     "inputs": '["table1", "table2"]',
            ...     "constants": '{"key": "value"}',
            ...     "outputs": '[{"table": "out", "mode": "append"}]'
            ... })
            >>> print(result.is_valid)
        
        **Example 4: Batch Validation with Report**
        
            >>> from odibi_de_v2.config import ConfigValidator
            >>> 
            >>> validator = ConfigValidator()
            >>> report = validator.generate_validation_report(all_configs)
            >>> 
            >>> print(f"Total: {report['total']}")
            >>> print(f"Valid: {report['valid']}")
            >>> print(f"Invalid: {report['invalid']}")
            >>> 
            >>> for issue in report['issues']:
            ...     print(f"- {issue['id']}: {issue['error']}")
    """
    
    def __init__(self, check_modules: bool = False):
        """
        Initialize validator.
        
        Args:
            check_modules: Whether to verify module paths exist (slower)
        """
        self.check_modules = check_modules
    
    def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single transformation configuration.
        
        Args:
            config: Configuration dictionary from TransformationRegistry
            
        Returns:
            ValidationResult with errors and warnings
        """
        errors = []
        warnings = []
        
        # Check required fields
        required = ['transformation_id', 'project', 'layer', 'module', 'function']
        for field in required:
            if not config.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate transformation_id format
        if config.get('transformation_id'):
            tid = config['transformation_id']
            if ' ' in tid:
                warnings.append(f"transformation_id contains spaces: '{tid}'")
        
        # Validate JSON fields
        json_result = self.validate_json_fields(config)
        errors.extend(json_result.errors)
        warnings.extend(json_result.warnings)
        
        # Validate module and function names
        if config.get('module'):
            module = config['module']
            if not all(part.isidentifier() or part == '' for part in module.split('.')):
                errors.append(f"Invalid module name: {module}")
            
            if self.check_modules:
                if not self._module_exists(module):
                    warnings.append(f"Module may not exist: {module}")
        
        if config.get('function'):
            func = config['function']
            if not func.isidentifier():
                errors.append(f"Invalid function name: {func}")
        
        # Validate layer format
        if config.get('layer'):
            layer = config['layer']
            valid_layers = ['Bronze', 'Silver', 'Gold']
            valid_layers_numbered = [f'{l}_{i}' for l in ['Silver', 'Gold'] for i in range(1, 10)]
            all_valid = valid_layers + valid_layers_numbered
            
            if layer not in all_valid:
                warnings.append(f"Non-standard layer name: {layer}")
        
        # Check for empty inputs/outputs
        inputs = self._parse_json_field(config.get('inputs'))
        outputs = self._parse_json_field(config.get('outputs'))
        
        if isinstance(inputs, list) and len(inputs) == 0:
            warnings.append("No inputs defined")
        
        if isinstance(outputs, list) and len(outputs) == 0:
            warnings.append("No outputs defined")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def validate_configs(self, configs: List[Dict[str, Any]]) -> List[ValidationResult]:
        """
        Validate multiple configurations.
        
        Args:
            configs: List of configuration dictionaries
            
        Returns:
            List of ValidationResults, one per config
        """
        return [self.validate_config(cfg) for cfg in configs]
    
    def validate_json_fields(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Validate only the JSON fields (inputs, constants, outputs).
        
        Args:
            config: Configuration with JSON fields
            
        Returns:
            ValidationResult for JSON fields only
        """
        errors = []
        warnings = []
        
        # Validate inputs
        inputs = self._parse_json_field(config.get('inputs'))
        if isinstance(inputs, str) and inputs.startswith('ERROR'):
            errors.append(f"inputs: {inputs}")
        elif isinstance(inputs, list):
            for i, inp in enumerate(inputs):
                if isinstance(inp, dict):
                    if 'type' in inp and inp['type'] == 'query':
                        if 'sql' not in inp:
                            errors.append(f"inputs[{i}]: query type missing 'sql' field")
                    elif 'table' not in inp and 'sql' not in inp:
                        warnings.append(f"inputs[{i}]: unclear input structure")
        
        # Validate constants
        constants = self._parse_json_field(config.get('constants'))
        if isinstance(constants, str) and constants.startswith('ERROR'):
            errors.append(f"constants: {constants}")
        elif isinstance(constants, dict):
            if len(constants) > 20:
                warnings.append(f"Large constants object ({len(constants)} keys)")
        
        # Validate outputs
        outputs = self._parse_json_field(config.get('outputs'))
        if isinstance(outputs, str) and outputs.startswith('ERROR'):
            errors.append(f"outputs: {outputs}")
        elif isinstance(outputs, list):
            for i, out in enumerate(outputs):
                if isinstance(out, dict):
                    if 'table' not in out:
                        errors.append(f"outputs[{i}]: missing 'table' field")
                    if 'mode' not in out:
                        warnings.append(f"outputs[{i}]: missing 'mode' field (will default to overwrite)")
                    elif out['mode'] not in ['overwrite', 'append', 'merge']:
                        warnings.append(f"outputs[{i}]: unusual mode '{out['mode']}'")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def generate_validation_report(self, configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report.
        
        Args:
            configs: List of configurations to validate
            
        Returns:
            Dictionary with validation statistics and issues
        """
        results = self.validate_configs(configs)
        
        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = len(results) - valid_count
        
        issues = []
        for cfg, result in zip(configs, results):
            if not result.is_valid or result.warnings:
                issues.append({
                    'id': cfg.get('transformation_id', 'unknown'),
                    'errors': result.errors,
                    'warnings': result.warnings
                })
        
        return {
            'total': len(configs),
            'valid': valid_count,
            'invalid': invalid_count,
            'issues': issues,
            'pass_rate': f"{(valid_count/len(configs)*100):.1f}%" if configs else "0%"
        }
    
    def _parse_json_field(self, field: Any) -> Any:
        """Parse JSON field, return error message if invalid"""
        if not field:
            return [] if isinstance(field, str) else field
        
        if isinstance(field, str):
            try:
                return json.loads(field)
            except json.JSONDecodeError as e:
                return f"ERROR: Invalid JSON - {str(e)}"
        
        return field
    
    def _module_exists(self, module_name: str) -> bool:
        """Check if a module exists"""
        try:
            spec = importlib.util.find_spec(module_name)
            return spec is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            return False


def validate_transformation_registry(configs: List[Dict[str, Any]]) -> None:
    """
    Quick validation of TransformationRegistry configs with console output.
    
    Args:
        configs: List of transformation configurations
        
    Examples:
        >>> from odibi_de_v2.config import validate_transformation_registry
        >>> 
        >>> # Validate all configs
        >>> validate_transformation_registry(all_configs)
    """
    validator = ConfigValidator()
    report = validator.generate_validation_report(configs)
    
    print("=" * 60)
    print("📋 TRANSFORMATION REGISTRY VALIDATION REPORT")
    print("=" * 60)
    print(f"Total configurations: {report['total']}")
    print(f"✅ Valid: {report['valid']}")
    print(f"❌ Invalid: {report['invalid']}")
    print(f"Pass rate: {report['pass_rate']}")
    print()
    
    if report['issues']:
        print(f"Issues found in {len(report['issues'])} configuration(s):")
        print()
        
        for issue in report['issues']:
            print(f"🔧 {issue['id']}")
            
            if issue['errors']:
                print("  Errors:")
                for err in issue['errors']:
                    print(f"    ❌ {err}")
            
            if issue['warnings']:
                print("  Warnings:")
                for warn in issue['warnings']:
                    print(f"    ⚠️  {warn}")
            
            print()
    else:
        print("✅ All configurations are valid!")
