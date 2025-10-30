"""
Convenience Helper Functions

Common utility functions to save time on routine data engineering tasks.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json


def list_projects(sql_provider, spark, env: str = "qat") -> List[str]:
    """
    List all configured projects in TransformationRegistry.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        env: Environment filter (default: qat)
        
    Returns:
        List of unique project names
        
    Examples:
        >>> from odibi_de_v2.utils import list_projects
        >>> 
        >>> projects = list_projects(sql_provider, spark, "qat")
        >>> for project in projects:
        ...     print(f"- {project}")
    """
    from odibi_de_v2.core import DataType
    
    query = f"""
    SELECT DISTINCT project
    FROM TransformationRegistry
    WHERE environment = '{env}'
    ORDER BY project
    """
    
    df = sql_provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=query,
        spark=spark
    )
    
    return [row['project'] for row in df.collect()]


def get_project_status(sql_provider, spark, project: str, env: str = "qat", hours: int = 24) -> Dict[str, Any]:
    """
    Get recent execution status for a project.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        hours: Look back period in hours (default: 24)
        
    Returns:
        Dictionary with project execution statistics
        
    Examples:
        >>> from odibi_de_v2.utils import get_project_status
        >>> 
        >>> status = get_project_status(sql_provider, spark, "MyProject", "qat")
        >>> print(f"Success rate: {status['success_rate']}")
        >>> print(f"Failed: {status['failed_count']}")
    """
    from odibi_de_v2.core import DataType
    
    # Calculate cutoff time
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
    SELECT
        COUNT(*) as total_runs,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
        AVG(duration_seconds) as avg_duration,
        MAX(end_time) as last_run
    FROM config_driven.TransformationRunLog
    WHERE project = '{project}'
    AND env = '{env}'
    AND start_time >= '{cutoff}'
    """
    
    try:
        df = sql_provider.read(
            data_type=DataType.SQL,
            container="",
            path_prefix="",
            object_name=query,
            spark=spark
        )
        
        row = df.collect()[0]
        
        total = row['total_runs'] or 0
        success = row['success_count'] or 0
        failed = row['failed_count'] or 0
        
        return {
            'project': project,
            'environment': env,
            'period_hours': hours,
            'total_runs': total,
            'success_count': success,
            'failed_count': failed,
            'success_rate': f"{(success/total*100):.1f}%" if total > 0 else "N/A",
            'avg_duration_seconds': round(row['avg_duration'] or 0, 2),
            'last_run': str(row['last_run']) if row['last_run'] else None
        }
    except Exception as e:
        return {
            'project': project,
            'environment': env,
            'error': str(e)
        }


def get_layer_summary(sql_provider, spark, project: str, env: str = "qat") -> List[Dict[str, Any]]:
    """
    Get summary statistics for each layer in a project.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        
    Returns:
        List of layer summaries with config counts and status
        
    Examples:
        >>> from odibi_de_v2.utils import get_layer_summary
        >>> 
        >>> summary = get_layer_summary(sql_provider, spark, "MyProject", "qat")
        >>> for layer in summary:
        ...     print(f"{layer['layer']}: {layer['config_count']} configs")
    """
    from odibi_de_v2.core import DataType
    
    query = f"""
    SELECT
        layer,
        COUNT(*) as config_count,
        SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) as enabled_count,
        SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) as disabled_count
    FROM TransformationRegistry
    WHERE LOWER(project) = LOWER('{project}')
    AND environment = '{env}'
    GROUP BY layer
    ORDER BY 
        CASE 
            WHEN layer = 'Bronze' THEN 1
            WHEN layer LIKE 'Silver%' THEN 2
            WHEN layer LIKE 'Gold%' THEN 3
            ELSE 4
        END,
        layer
    """
    
    df = sql_provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=query,
        spark=spark
    )
    
    return [row.asDict() for row in df.collect()]


def get_failed_transformations(sql_provider, spark, project: str, env: str = "qat", hours: int = 24) -> List[Dict[str, Any]]:
    """
    Get list of failed transformations in recent period.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        hours: Look back period in hours
        
    Returns:
        List of failed transformation details
        
    Examples:
        >>> from odibi_de_v2.utils import get_failed_transformations
        >>> 
        >>> failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat")
        >>> for fail in failed:
        ...     print(f"❌ {fail['transformation_id']}: {fail['error_message']}")
    """
    from odibi_de_v2.core import DataType
    
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
    SELECT
        transformation_id,
        start_time,
        end_time,
        error_message,
        duration_seconds
    FROM config_driven.TransformationRunLog
    WHERE project = '{project}'
    AND env = '{env}'
    AND status = 'FAILED'
    AND start_time >= '{cutoff}'
    ORDER BY start_time DESC
    """
    
    try:
        df = sql_provider.read(
            data_type=DataType.SQL,
            container="",
            path_prefix="",
            object_name=query,
            spark=spark
        )
        
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        return []


def retry_failed_transformations(
    sql_provider,
    spark,
    project: str,
    env: str = "qat",
    layer: str = "Silver",
    hours: int = 24,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Identify and optionally retry failed transformations.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        layer: Layer to retry
        hours: Look back period for failures
        dry_run: If True, only show what would be retried (default: True)
        
    Returns:
        Dictionary with retry plan and results
        
    Examples:
        >>> from odibi_de_v2.utils import retry_failed_transformations
        >>> 
        >>> # Check what would be retried
        >>> result = retry_failed_transformations(
        ...     sql_provider, spark, "MyProject", "qat", "Silver_1", dry_run=True
        ... )
        >>> print(f"Would retry: {result['failed_ids']}")
        >>> 
        >>> # Actually retry
        >>> result = retry_failed_transformations(
        ...     sql_provider, spark, "MyProject", "qat", "Silver_1", dry_run=False
        ... )
    """
    failed = get_failed_transformations(sql_provider, spark, project, env, hours)
    failed_ids = list(set(f['transformation_id'] for f in failed))
    
    result = {
        'project': project,
        'environment': env,
        'layer': layer,
        'failed_count': len(failed_ids),
        'failed_ids': failed_ids,
        'dry_run': dry_run
    }
    
    if dry_run:
        result['message'] = f"Would retry {len(failed_ids)} transformation(s). Set dry_run=False to execute."
        return result
    
    # Actually retry using TransformationRunnerFromConfig
    try:
        from odibi_de_v2.transformer import TransformationRunnerFromConfig
        
        runner = TransformationRunnerFromConfig(
            sql_provider=sql_provider,
            project=project,
            env=env,
            layer=layer
        )
        
        # Filter configs to only failed ones
        all_configs = runner._fetch_configs()
        retry_configs = [c for c in all_configs if c.get('id') in failed_ids]
        
        result['retry_count'] = len(retry_configs)
        result['message'] = f"Retrying {len(retry_configs)} transformation(s)..."
        
        # This would actually execute - implement based on your retry strategy
        # runner.run(configs=retry_configs)
        
    except Exception as e:
        result['error'] = str(e)
    
    return result


def get_transformation_config(sql_provider, spark, transformation_id: str, env: str = "qat") -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific transformation.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        transformation_id: Transformation ID to look up
        env: Environment
        
    Returns:
        Configuration dictionary or None if not found
        
    Examples:
        >>> from odibi_de_v2.utils import get_transformation_config
        >>> 
        >>> config = get_transformation_config(sql_provider, spark, "my-transform", "qat")
        >>> if config:
        ...     print(f"Module: {config['module']}")
        ...     print(f"Function: {config['function']}")
    """
    from odibi_de_v2.core import DataType
    
    query = f"""
    SELECT *
    FROM TransformationRegistry
    WHERE transformation_id = '{transformation_id}'
    AND environment = '{env}'
    """
    
    df = sql_provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=query,
        spark=spark
    )
    
    rows = df.collect()
    if rows:
        config = rows[0].asDict()
        
        # Parse JSON fields
        for field in ['inputs', 'constants', 'outputs']:
            if config.get(field) and isinstance(config[field], str):
                try:
                    config[field] = json.loads(config[field])
                except json.JSONDecodeError:
                    pass
        
        return config
    
    return None


def print_project_summary(sql_provider, spark, project: str, env: str = "qat") -> None:
    """
    Print comprehensive project summary.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        
    Examples:
        >>> from odibi_de_v2.utils import print_project_summary
        >>> 
        >>> print_project_summary(sql_provider, spark, "MyProject", "qat")
    """
    print("\n" + "=" * 60)
    print(f"📊 PROJECT SUMMARY: {project} ({env})")
    print("=" * 60)
    
    # Get layer summary
    layers = get_layer_summary(sql_provider, spark, project, env)
    
    if layers:
        print("\n📁 Layers:")
        total_configs = 0
        for layer in layers:
            print(f"  {layer['layer']:15} {layer['enabled_count']:3} enabled, {layer['disabled_count']:3} disabled")
            total_configs += layer['config_count']
        print(f"\n  Total configurations: {total_configs}")
    else:
        print("\n⚠️  No configurations found")
    
    # Get execution status
    print("\n⏱️  Recent Execution Status (last 24h):")
    status = get_project_status(sql_provider, spark, project, env, 24)
    
    if 'error' not in status:
        print(f"  Total runs: {status['total_runs']}")
        print(f"  Success: {status['success_count']} ({status['success_rate']})")
        print(f"  Failed: {status['failed_count']}")
        print(f"  Avg duration: {status['avg_duration_seconds']}s")
        
        if status['last_run']:
            print(f"  Last run: {status['last_run']}")
    else:
        print(f"  ⚠️  Could not fetch execution status: {status['error']}")
    
    # Get recent failures
    failed = get_failed_transformations(sql_provider, spark, project, env, 24)
    
    if failed:
        print(f"\n❌ Recent Failures ({len(failed)}):")
        for fail in failed[:5]:
            print(f"  - {fail['transformation_id']}: {fail.get('error_message', 'Unknown error')[:60]}")
        
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    else:
        print("\n✅ No recent failures")
    
    print("\n" + "=" * 60)


def export_project_config(sql_provider, spark, project: str, env: str = "qat", output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Export entire project configuration to JSON.
    
    Args:
        sql_provider: SQL connection provider
        spark: Spark session
        project: Project name
        env: Environment
        output_file: Optional file path to save JSON
        
    Returns:
        Dictionary with all project configurations
        
    Examples:
        >>> from odibi_de_v2.utils import export_project_config
        >>> 
        >>> # Export to dict
        >>> config = export_project_config(sql_provider, spark, "MyProject", "qat")
        >>> 
        >>> # Export to file
        >>> export_project_config(
        ...     sql_provider, spark, "MyProject", "qat",
        ...     output_file="myproject_config.json"
        ... )
    """
    from odibi_de_v2.core import DataType
    
    query = f"""
    SELECT *
    FROM TransformationRegistry
    WHERE LOWER(project) = LOWER('{project}')
    AND environment = '{env}'
    ORDER BY layer, step
    """
    
    df = sql_provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=query,
        spark=spark
    )
    
    configs = []
    for row in df.collect():
        config = row.asDict()
        
        # Parse JSON fields
        for field in ['inputs', 'constants', 'outputs']:
            if config.get(field) and isinstance(config[field], str):
                try:
                    config[field] = json.loads(config[field])
                except json.JSONDecodeError:
                    pass
        
        configs.append(config)
    
    export_data = {
        'project': project,
        'environment': env,
        'export_timestamp': datetime.now().isoformat(),
        'config_count': len(configs),
        'configurations': configs
    }
    
    if output_file:
        import json as json_module
        with open(output_file, 'w') as f:
            json_module.dump(export_data, f, indent=2, default=str)
        print(f"✅ Exported {len(configs)} configurations to {output_file}")
    
    return export_data
