"""
Project CLI - Command-line helper for common data engineering tasks

Quick commands for managing odibi_de_v2 projects without writing code.
"""

import argparse
import sys
from typing import Optional
import json


class ProjectCLI:
    """
    Command-line interface for odibi_de_v2 project management.
    
    Provides quick commands for:
    - Listing all projects
    - Checking project health
    - Generating config templates
    - Viewing project status
    - Exporting configurations
    
    Examples:
        **Example 1: CLI Usage**
        
            ```bash
            # List all projects
            python -m odibi_de_v2.cli.project_cli list --env qat
            
            # Check project health
            python -m odibi_de_v2.cli.project_cli health --project "MyProject" --env qat
            
            # Show project summary
            python -m odibi_de_v2.cli.project_cli summary --project "MyProject"
            
            # Generate config template
            python -m odibi_de_v2.cli.project_cli template --transformation-id "new-transform"
            
            # Export project config
            python -m odibi_de_v2.cli.project_cli export --project "MyProject" --output config.json
            ```
        
        **Example 2: Programmatic Usage**
        
            >>> from odibi_de_v2.cli import ProjectCLI
            >>> 
            >>> cli = ProjectCLI(sql_provider, spark)
            >>> cli.list_projects("qat")
            >>> cli.check_health("MyProject", "qat")
    """
    
    def __init__(self, sql_provider=None, spark=None):
        """
        Initialize CLI.
        
        Args:
            sql_provider: SQL connection provider
            spark: Spark session
        """
        self.sql_provider = sql_provider
        self.spark = spark
    
    def list_projects(self, env: str = "qat") -> None:
        """List all configured projects"""
        from odibi_de_v2.utils.helpers import list_projects
        
        try:
            projects = list_projects(self.sql_provider, self.spark, env)
            
            print(f"\n📁 Projects in {env} environment:")
            print("=" * 40)
            
            if projects:
                for i, project in enumerate(projects, 1):
                    print(f"{i:2}. {project}")
                print(f"\nTotal: {len(projects)} project(s)")
            else:
                print("No projects found")
            
            print()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def check_health(self, project: str, env: str = "qat") -> None:
        """Run health check on a project"""
        from odibi_de_v2.utils.health_check import HealthCheck
        
        try:
            hc = HealthCheck(self.sql_provider, self.spark, project, env)
            report = hc.run_full_check()
            hc.print_report(report)
            
            # Exit with error code if not healthy
            if report['overall_health'] in ['CRITICAL', 'ERROR']:
                sys.exit(1)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def show_summary(self, project: str, env: str = "qat") -> None:
        """Show project summary"""
        from odibi_de_v2.utils.helpers import print_project_summary
        
        try:
            print_project_summary(self.sql_provider, self.spark, project, env)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def export_config(self, project: str, env: str = "qat", output: Optional[str] = None) -> None:
        """Export project configuration"""
        from odibi_de_v2.utils.helpers import export_project_config
        
        try:
            config = export_project_config(
                self.sql_provider,
                self.spark,
                project,
                env,
                output
            )
            
            if not output:
                print(json.dumps(config, indent=2, default=str))
                
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def validate_config(self, project: str, env: str = "qat") -> None:
        """Validate project configurations"""
        from odibi_de_v2.config.validator import ConfigValidator
        from odibi_de_v2.core import DataType
        
        try:
            # Load configs
            query = f"""
            SELECT *
            FROM TransformationRegistry
            WHERE LOWER(project) = LOWER('{project}')
            AND environment = '{env}'
            """
            
            df = self.sql_provider.read(
                data_type=DataType.SQL,
                container="",
                path_prefix="",
                object_name=query,
                spark=self.spark
            )
            
            configs = [row.asDict() for row in df.collect()]
            
            # Validate
            validator = ConfigValidator()
            report = validator.generate_validation_report(configs)
            
            print("\n" + "=" * 60)
            print("📋 CONFIGURATION VALIDATION REPORT")
            print("=" * 60)
            print(f"Project: {project}")
            print(f"Environment: {env}")
            print(f"Total: {report['total']}")
            print(f"✅ Valid: {report['valid']}")
            print(f"❌ Invalid: {report['invalid']}")
            print(f"Pass rate: {report['pass_rate']}")
            print()
            
            if report['issues']:
                print(f"Issues found in {len(report['issues'])} configuration(s):")
                print()
                
                for issue in report['issues'][:10]:
                    print(f"🔧 {issue['id']}")
                    
                    if issue['errors']:
                        for err in issue['errors']:
                            print(f"   ❌ {err}")
                    
                    if issue['warnings']:
                        for warn in issue['warnings']:
                            print(f"   ⚠️  {warn}")
                    
                    print()
                
                if len(report['issues']) > 10:
                    print(f"... and {len(report['issues']) - 10} more issues")
            
            print("=" * 60)
            
            # Exit with error if invalid configs found
            if report['invalid'] > 0:
                sys.exit(1)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def generate_template(self, transformation_id: str) -> None:
        """Generate configuration template"""
        template = {
            "transformation_id": transformation_id,
            "transformation_group_id": "",
            "project": "YOUR_PROJECT",
            "environment": "qat",
            "layer": "Silver_1",
            "step": 1,
            "enabled": 1,
            "entity_1": "",
            "entity_2": "",
            "entity_3": "",
            "module": "your_project.transformations.silver",
            "function": "your_function_name",
            "inputs": ["input_table_1"],
            "constants": {},
            "outputs": [
                {
                    "table": "output_table",
                    "mode": "overwrite"
                }
            ],
            "description": "Description of what this transformation does"
        }
        
        print("\n" + "=" * 60)
        print("📝 TRANSFORMATION CONFIG TEMPLATE")
        print("=" * 60)
        print()
        print(json.dumps(template, indent=2))
        print()
        print("=" * 60)
        print("\n💡 Tip: Copy this template and customize for your needs")
        print("   Use TransformationRegistryUI in notebooks for interactive editing\n")
    
    def show_failed(self, project: str, env: str = "qat", hours: int = 24) -> None:
        """Show failed transformations"""
        from odibi_de_v2.utils.helpers import get_failed_transformations
        
        try:
            failed = get_failed_transformations(
                self.sql_provider,
                self.spark,
                project,
                env,
                hours
            )
            
            print(f"\n❌ Failed Transformations (last {hours}h)")
            print("=" * 60)
            
            if failed:
                for fail in failed:
                    print(f"\n🔴 {fail['transformation_id']}")
                    print(f"   Time: {fail['start_time']}")
                    print(f"   Duration: {fail.get('duration_seconds', 'N/A')}s")
                    print(f"   Error: {fail.get('error_message', 'Unknown')[:100]}")
                
                print(f"\nTotal failures: {len(failed)}")
            else:
                print("\n✅ No failures in this period")
            
            print()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="odibi_de_v2 Project CLI - Quick commands for data engineering tasks",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all projects')
    list_parser.add_argument('--env', default='qat', help='Environment (qat, prod, dev)')
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Check project health')
    health_parser.add_argument('--project', required=True, help='Project name')
    health_parser.add_argument('--env', default='qat', help='Environment')
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Show project summary')
    summary_parser.add_argument('--project', required=True, help='Project name')
    summary_parser.add_argument('--env', default='qat', help='Environment')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export project configuration')
    export_parser.add_argument('--project', required=True, help='Project name')
    export_parser.add_argument('--env', default='qat', help='Environment')
    export_parser.add_argument('--output', help='Output file path (JSON)')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configurations')
    validate_parser.add_argument('--project', required=True, help='Project name')
    validate_parser.add_argument('--env', default='qat', help='Environment')
    
    # Template command
    template_parser = subparsers.add_parser('template', help='Generate config template')
    template_parser.add_argument('--transformation-id', required=True, help='Transformation ID')
    
    # Failed command
    failed_parser = subparsers.add_parser('failed', help='Show failed transformations')
    failed_parser.add_argument('--project', required=True, help='Project name')
    failed_parser.add_argument('--env', default='qat', help='Environment')
    failed_parser.add_argument('--hours', type=int, default=24, help='Look back hours')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize CLI
    # Note: In real usage, you'd need to initialize sql_provider and spark
    # This is a standalone CLI, so it would need connection setup
    try:
        from pyspark.sql import SparkSession
        from odibi_de_v2.connector import SQLServerProvider
        
        spark = SparkSession.getActiveSession()
        if not spark:
            print("❌ No active Spark session. Run this in Databricks or with Spark initialized.")
            sys.exit(1)
        
        # You'd need to configure SQL provider based on your setup
        sql_provider = None  # Initialize based on your connection details
        
        cli = ProjectCLI(sql_provider, spark)
        
        # Execute command
        if args.command == 'list':
            cli.list_projects(args.env)
        
        elif args.command == 'health':
            cli.check_health(args.project, args.env)
        
        elif args.command == 'summary':
            cli.show_summary(args.project, args.env)
        
        elif args.command == 'export':
            cli.export_config(args.project, args.env, args.output)
        
        elif args.command == 'validate':
            cli.validate_config(args.project, args.env)
        
        elif args.command == 'template':
            cli.generate_template(args.transformation_id)
        
        elif args.command == 'failed':
            cli.show_failed(args.project, args.env, args.hours)
        
    except ImportError as e:
        print(f"❌ Missing dependencies: {e}")
        print("💡 Make sure you're running in a Databricks/Spark environment")
        sys.exit(1)
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
