from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow
def dbt_command_flow(sub_cmd: str, models: str):

    """
    programmatic manner of constructing this dbt cli command:
    dbt run --profiles-dir profiles/ --select <MODEL_NAME>
    """

    dbt_command_dict = {
        "command": f"dbt {sub_cmd} --select {models}",
        "profiles_dir": "~/.dbt/",
        "project_dir": "./ints/dbt/flows/"
    }

    result = trigger_dbt_cli_command(**dbt_command_dict)

    return result

if __name__ == "__main__":
    dbt_command_flow("run", "stage_dummyjson__products")
