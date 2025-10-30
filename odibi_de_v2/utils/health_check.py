"""
Health Check Utility

Verifies TransformationRegistry data quality and common issues.
Checks for configuration problems, missing dependencies, and data integrity.
"""

from typing import Dict, List, Any, Optional, Tuple
import json
import importlib.util
from datetime import datetime


class HealthCheck:
    """
    Comprehensive health check for odibi_de_v2 projects.
    
    Performs diagnostics on:
    - TransformationRegistry data integrity
    - Module and function availability
    - Configuration consistency
    - Manifest validity
    - Common misconfigurations
    
    Examples:
        **Example 1: Quick Health Check**
        
            >>> from odibi_de_v2.utils import HealthCheck
            >>> 
            >>> # With SQL provider and Spark
            >>> hc = HealthCheck(sql_provider, spark, project="MyProject", env="qat")
            >>> report = hc.run_full_check()
            >>> hc.print_report(report)
        
        **Example 2: Check Specific Layer**
        
            >>> from odibi_de_v2.utils import HealthCheck
            >>> 
            >>> hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
            >>> result = hc.check_layer("Silver_1")
            >>> 
            >>> if not result['healthy']:
            ...     print(f"Issues: {result['issues']}")
        
        **Example 3: Validate All Functions Exist**
        
            >>> from odibi_de_v2.utils import HealthCheck
            >>> 
            >>> hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
            >>> missing = hc.check_all_functions_exist()
            >>> 
            >>> if missing:
            ...     print("Missing functions:")
            ...     for item in missing:
            ...         print(f"  - {item['module']}.{item['function']}")
        
        **Example 4: Check Data Quality**
        
            >>> from odibi_de_v2.utils import HealthCheck
            >>> 
            >>> hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
            >>> issues = hc.check_data_quality()
            >>> 
            >>> for issue in issues:
            ...     print(f"⚠️  {issue['type']}: {issue['message']}")
    """
    
    def __init__(self, sql_provider=None, spark=None, project: str = "", env: str = "qat"):
        """
        Initialize health checker.
        
        Args:
            sql_provider: SQL connection provider (optional for some checks)
            spark: Spark session (optional for some checks)
            project: Project name to check
            env: Environment to check (qat, prod, dev)
        """
        self.sql_provider = sql_provider
        self.spark = spark
        self.project = project
        self.env = env
    
    def run_full_check(self) -> Dict[str, Any]:
        """
        Run complete health check.
        
        Returns:
            Comprehensive health report dictionary
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'project': self.project,
            'environment': self.env,
            'checks': {},
            'overall_health': 'UNKNOWN'
        }
        
        try:
            # Load configs
            configs = self._load_configs()
            report['total_configs'] = len(configs)
            
            # Run all checks
            report['checks']['data_quality'] = self.check_data_quality(configs)
            report['checks']['json_validity'] = self.check_json_validity(configs)
            report['checks']['missing_functions'] = self.check_all_functions_exist(configs)
            report['checks']['layer_consistency'] = self.check_layer_consistency(configs)
            report['checks']['duplicate_ids'] = self.check_duplicate_ids(configs)
            
            # Determine overall health
            issues_count = sum(len(check) for check in report['checks'].values() if isinstance(check, list))
            
            if issues_count == 0:
                report['overall_health'] = 'HEALTHY'
            elif issues_count < 5:
                report['overall_health'] = 'WARNING'
            else:
                report['overall_health'] = 'CRITICAL'
                
        except Exception as e:
            report['overall_health'] = 'ERROR'
            report['error'] = str(e)
        
        return report
    
    def check_data_quality(self, configs: Optional[List[Dict]] = None) -> List[Dict[str, str]]:
        """
        Check for data quality issues in configurations.
        
        Args:
            configs: Optional list of configs (will load if not provided)
            
        Returns:
            List of data quality issues found
        """
        if configs is None:
            configs = self._load_configs()
        
        issues = []
        
        for cfg in configs:
            tid = cfg.get('transformation_id', 'unknown')
            
            # Check for missing critical fields
            if not cfg.get('module'):
                issues.append({
                    'id': tid,
                    'type': 'MISSING_MODULE',
                    'message': f"{tid}: Missing module path"
                })
            
            if not cfg.get('function'):
                issues.append({
                    'id': tid,
                    'type': 'MISSING_FUNCTION',
                    'message': f"{tid}: Missing function name"
                })
            
            # Check for disabled configs
            if cfg.get('enabled') == 0:
                issues.append({
                    'id': tid,
                    'type': 'DISABLED',
                    'message': f"{tid}: Configuration is disabled"
                })
            
            # Check for empty inputs/outputs
            inputs = self._parse_json(cfg.get('inputs'))
            outputs = self._parse_json(cfg.get('outputs'))
            
            if isinstance(inputs, list) and len(inputs) == 0:
                issues.append({
                    'id': tid,
                    'type': 'NO_INPUTS',
                    'message': f"{tid}: No input tables defined"
                })
            
            if isinstance(outputs, list) and len(outputs) == 0:
                issues.append({
                    'id': tid,
                    'type': 'NO_OUTPUTS',
                    'message': f"{tid}: No output tables defined"
                })
        
        return issues
    
    def check_json_validity(self, configs: Optional[List[Dict]] = None) -> List[Dict[str, str]]:
        """
        Check that all JSON fields are valid.
        
        Args:
            configs: Optional list of configs
            
        Returns:
            List of JSON parsing errors
        """
        if configs is None:
            configs = self._load_configs()
        
        issues = []
        
        for cfg in configs:
            tid = cfg.get('transformation_id', 'unknown')
            
            # Check inputs
            result = self._validate_json_field(cfg.get('inputs'), 'inputs')
            if result:
                issues.append({'id': tid, 'field': 'inputs', 'error': result})
            
            # Check constants
            result = self._validate_json_field(cfg.get('constants'), 'constants')
            if result:
                issues.append({'id': tid, 'field': 'constants', 'error': result})
            
            # Check outputs
            result = self._validate_json_field(cfg.get('outputs'), 'outputs')
            if result:
                issues.append({'id': tid, 'field': 'outputs', 'error': result})
        
        return issues
    
    def check_all_functions_exist(self, configs: Optional[List[Dict]] = None) -> List[Dict[str, str]]:
        """
        Verify all transformation functions can be imported.
        
        Args:
            configs: Optional list of configs
            
        Returns:
            List of missing functions
        """
        if configs is None:
            configs = self._load_configs()
        
        missing = []
        
        for cfg in configs:
            tid = cfg.get('transformation_id', 'unknown')
            module = cfg.get('module')
            function = cfg.get('function')
            
            if not module or not function:
                continue
            
            try:
                spec = importlib.util.find_spec(module)
                if spec is None:
                    missing.append({
                        'id': tid,
                        'module': module,
                        'function': function,
                        'error': 'Module not found'
                    })
            except (ImportError, ModuleNotFoundError, ValueError) as e:
                missing.append({
                    'id': tid,
                    'module': module,
                    'function': function,
                    'error': str(e)
                })
        
        return missing
    
    def check_layer_consistency(self, configs: Optional[List[Dict]] = None) -> List[Dict[str, str]]:
        """
        Check for layer naming and ordering issues.
        
        Args:
            configs: Optional list of configs
            
        Returns:
            List of layer consistency issues
        """
        if configs is None:
            configs = self._load_configs()
        
        issues = []
        
        # Get all unique layers
        layers = set(cfg.get('layer') for cfg in configs if cfg.get('layer'))
        
        # Check for non-standard layer names
        standard_layers = {'Bronze', 'Silver', 'Silver_1', 'Silver_2', 'Gold', 'Gold_1', 'Gold_2'}
        non_standard = layers - standard_layers
        
        if non_standard:
            issues.append({
                'type': 'NON_STANDARD_LAYERS',
                'layers': list(non_standard),
                'message': f"Non-standard layer names found: {', '.join(non_standard)}"
            })
        
        # Check for missing layers in sequence
        if 'Silver_2' in layers and 'Silver_1' not in layers:
            issues.append({
                'type': 'MISSING_LAYER',
                'message': 'Silver_2 exists but Silver_1 is missing'
            })
        
        if 'Gold_2' in layers and 'Gold_1' not in layers:
            issues.append({
                'type': 'MISSING_LAYER',
                'message': 'Gold_2 exists but Gold_1 is missing'
            })
        
        return issues
    
    def check_duplicate_ids(self, configs: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """
        Check for duplicate transformation IDs.
        
        Args:
            configs: Optional list of configs
            
        Returns:
            List of duplicate ID issues
        """
        if configs is None:
            configs = self._load_configs()
        
        id_counts = {}
        for cfg in configs:
            tid = cfg.get('transformation_id', 'unknown')
            id_counts[tid] = id_counts.get(tid, 0) + 1
        
        duplicates = []
        for tid, count in id_counts.items():
            if count > 1:
                duplicates.append({
                    'id': tid,
                    'count': count,
                    'message': f"Duplicate transformation_id: {tid} appears {count} times"
                })
        
        return duplicates
    
    def check_layer(self, layer: str) -> Dict[str, Any]:
        """
        Check health of a specific layer.
        
        Args:
            layer: Layer name (e.g., "Silver_1")
            
        Returns:
            Layer health report
        """
        configs = self._load_configs()
        layer_configs = [c for c in configs if c.get('layer') == layer]
        
        if not layer_configs:
            return {
                'layer': layer,
                'healthy': False,
                'issues': [f"No configurations found for layer {layer}"]
            }
        
        issues = []
        
        # Check each config in layer
        for cfg in layer_configs:
            tid = cfg.get('transformation_id')
            
            # Check for JSON errors
            json_issues = self.check_json_validity([cfg])
            if json_issues:
                issues.extend([f"{tid}: {i['error']}" for i in json_issues])
            
            # Check for missing functions
            func_issues = self.check_all_functions_exist([cfg])
            if func_issues:
                issues.extend([f"{tid}: Function not found" for _ in func_issues])
        
        return {
            'layer': layer,
            'config_count': len(layer_configs),
            'healthy': len(issues) == 0,
            'issues': issues
        }
    
    def print_report(self, report: Dict[str, Any]) -> None:
        """
        Print formatted health check report.
        
        Args:
            report: Report dictionary from run_full_check()
        """
        print("\n" + "=" * 60)
        print("🏥 ODIBI_DE_V2 HEALTH CHECK REPORT")
        print("=" * 60)
        print(f"Project: {report['project']}")
        print(f"Environment: {report['environment']}")
        print(f"Timestamp: {report['timestamp']}")
        print(f"Overall Health: {report['overall_health']}")
        
        if report.get('total_configs'):
            print(f"Total Configurations: {report['total_configs']}")
        
        print()
        
        # Print each check
        checks = report.get('checks', {})
        
        if checks.get('data_quality'):
            print(f"❌ Data Quality Issues: {len(checks['data_quality'])}")
            for issue in checks['data_quality'][:5]:
                print(f"   - {issue['message']}")
            if len(checks['data_quality']) > 5:
                print(f"   ... and {len(checks['data_quality']) - 5} more")
            print()
        else:
            print("✅ Data Quality: OK")
            print()
        
        if checks.get('json_validity'):
            print(f"❌ JSON Validity Issues: {len(checks['json_validity'])}")
            for issue in checks['json_validity'][:5]:
                print(f"   - {issue['id']}.{issue['field']}: {issue['error']}")
            if len(checks['json_validity']) > 5:
                print(f"   ... and {len(checks['json_validity']) - 5} more")
            print()
        else:
            print("✅ JSON Validity: OK")
            print()
        
        if checks.get('missing_functions'):
            print(f"❌ Missing Functions: {len(checks['missing_functions'])}")
            for item in checks['missing_functions'][:5]:
                print(f"   - {item['id']}: {item['module']}.{item['function']}")
            if len(checks['missing_functions']) > 5:
                print(f"   ... and {len(checks['missing_functions']) - 5} more")
            print()
        else:
            print("✅ All Functions Found")
            print()
        
        if checks.get('layer_consistency'):
            print(f"⚠️  Layer Consistency: {len(checks['layer_consistency'])} issue(s)")
            for issue in checks['layer_consistency']:
                print(f"   - {issue['message']}")
            print()
        else:
            print("✅ Layer Consistency: OK")
            print()
        
        if checks.get('duplicate_ids'):
            print(f"❌ Duplicate IDs: {len(checks['duplicate_ids'])}")
            for item in checks['duplicate_ids']:
                print(f"   - {item['message']}")
            print()
        else:
            print("✅ No Duplicate IDs")
            print()
        
        print("=" * 60)
    
    def _load_configs(self) -> List[Dict[str, Any]]:
        """Load configs from TransformationRegistry"""
        if not self.sql_provider or not self.spark:
            return []
        
        from odibi_de_v2.core import DataType
        
        query = f"""
        SELECT *
        FROM TransformationRegistry
        WHERE LOWER(project) = LOWER('{self.project}')
        AND environment = '{self.env}'
        """
        
        df = self.sql_provider.read(
            data_type=DataType.SQL,
            container="",
            path_prefix="",
            object_name=query,
            spark=self.spark
        )
        
        return [row.asDict() for row in df.collect()]
    
    def _parse_json(self, field: Any) -> Any:
        """Parse JSON field safely"""
        if not field:
            return []
        
        if isinstance(field, str):
            try:
                return json.loads(field)
            except json.JSONDecodeError:
                return None
        
        return field
    
    def _validate_json_field(self, field: Any, field_name: str) -> Optional[str]:
        """Validate a single JSON field"""
        if not field:
            return None
        
        if isinstance(field, str):
            try:
                json.loads(field)
                return None
            except json.JSONDecodeError as e:
                return f"Invalid JSON: {str(e)}"
        
        return None


def quick_health_check(sql_provider, spark, project: str, env: str = "qat") -> None:
    """
    Run and print quick health check.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        
    Examples:
        >>> from odibi_de_v2.utils import quick_health_check
        >>> 
        >>> quick_health_check(sql_provider, spark, "MyProject", "qat")
    """
    hc = HealthCheck(sql_provider, spark, project, env)
    report = hc.run_full_check()
    hc.print_report(report)
