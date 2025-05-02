import os
from datetime import datetime
from pathlib import Path
import nbformat as nbf


def create_bronze_template(
    project: str,
    table: str,
    domain: str,
    source_id: str,
    target_id: str,
    email_to: str,
    logic_app_url: str,
    environment: str,  # "dev", "prod", or "both"
    template_output_folder: str ,
    environment_output_folder: str ,
    environment_filename: str ,  # custom runner filename for env
    app_name: str,
    secret_scope: str,
    blob_name_key: str,
    blob_key_key: str,
    sql_server_key: str,
    sql_db_key: str,
    sql_user_key: str,
    sql_pass_key: str,
    spark_config: dict ,
    generate_readme: bool ,
    name_override: str
):
    today = datetime.today().strftime("%Y%m%d")
    folder_name = name_override or f"{project}_{table}"
    base_filename = "bronze_runner"
    runner_py_name = f"{base_filename}.py"
    runner_ipynb_name = f"{base_filename}.ipynb"
    archive_py_name = f"{base_filename}_{today}.py"
    archive_ipynb_name = f"{base_filename}_{today}.ipynb"

    spark_config_str = "    spark_config={\n" + "\n".join(
        [f'        "{k}": "{v}",' for k, v in (spark_config or {}).items()]
    ) + "\n    }" if spark_config else "    spark_config={}"

    runner_code = f'''from odibi_de_v2.databricks import run_bronze_pipeline

logic_app_url = "{logic_app_url}"

logs = run_bronze_pipeline(
    project="{project}",
    table="{table}",
    domain="{domain}",
    source_id="{source_id}",
    target_id="{target_id}",
    app_name="{app_name or f'{project}_{table}_bronze_ingestion'}",
    secret_scope="{secret_scope}",
    blob_name_key="{blob_name_key}",
    blob_key_key="{blob_key_key}",
    sql_server_key="{sql_server_key}",
    sql_db_key="{sql_db_key}",
    sql_user_key="{sql_user_key}",
    sql_pass_key="{sql_pass_key}",
    send_email=True,
    email_to="{email_to}",
    logic_app_url=logic_app_url,
{spark_config_str}
)
'''

    # === Handle TEMPLATE Generation ===
    if environment == "both":
        template_path = Path(template_output_folder) / domain / "Bronze" / folder_name
        archive_path = template_path / "archive"
        template_path.mkdir(parents=True, exist_ok=True)
        archive_path.mkdir(parents=True, exist_ok=True)

        # Archive old versions
        for file_name, archive_name in [
            (runner_py_name, archive_py_name),
            (runner_ipynb_name, archive_ipynb_name)
        ]:
            file_path = template_path / file_name
            if file_path.exists():
                file_path.rename(archive_path / archive_name)

        # Write new versions
        (template_path / runner_py_name).write_text(runner_code)

        nb = nbf.v4.new_notebook()
        nb.cells = [nbf.v4.new_code_cell(runner_code)]
        with open(template_path / runner_ipynb_name, "w") as f:
            nbf.write(nb, f)

        if generate_readme:
            readme_path = template_path / "README.md"
            readme_path.write_text(
                f"# Bronze Runner Template\n\n"
                f"**Project:** {project}\n"
                f"**Table:** {table}\n"
                f"**Domain:** {domain}\n"
                f"Generated on {today} using `create_bronze_template()`."
            )

    # === Handle ENVIRONMENT-SPECIFIC Runner File ===
    if environment in ("dev", "prod"):
        env_path = Path(environment_output_folder)
        env_path.mkdir(parents=True, exist_ok=True)

        filename = environment_filename or f"{project.lower()}_{table.lower()}_runner"
        env_file_path = env_path / f"{filename}.{'ipynb' if environment == 'dev' else 'py'}"

        if environment == "prod":
            env_file_path.write_text(runner_code)
        elif environment == "dev":
            nb = nbf.v4.new_notebook()
            nb.cells = [nbf.v4.new_code_cell(runner_code)]
            with open(env_file_path, "w") as f:
                nbf.write(nb, f)

    return "Template created" if environment == "both" else "Environment runner created"
