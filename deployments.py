from flows.api import export
from flows.dbt import dbt_command_flow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule


WORK_QUEUE_CONCURRENCY = 1



def deploy_factory(name: str, flow, param: dict, cron: str = "") -> Deployment:
    kwargs = {
        "flow": flow,
        "name": name,
        "parameters": param,
        "work_queue_name": f"local_host",
        "output": f"{name}.yaml",
        "skip_upload": True,
    }
    if cron:
        kwargs["schedule"] = CronSchedule(cron=cron)

    return Deployment.build_from_flow(**kwargs)


dummyjson_products_dep = deploy_factory(
    name="dummyjson_products",
    flow=export,
    param={},
    cron="0 0 * * *",
)
dummyjson_products_dep.apply(work_queue_concurrency=WORK_QUEUE_CONCURRENCY)

dbt_downstream_model = deploy_factory(
    name="dbt_downstream_model",
    flow=dbt_command_flow,
    param={"sub_cmd": "run", "models": "stage_dummyjson__products"},
    cron="5 0 * * *",
)
dbt_downstream_model.apply(work_queue_concurrency=WORK_QUEUE_CONCURRENCY)
