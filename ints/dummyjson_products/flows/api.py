import json
import requests

from prefect import flow, task
from prefect.deployments import run_deployment



@flow
def export():
    resp = requests.get("https://dummyjson.com/products")
    resp.raise_for_status()
    list_of_prods = resp.json()["products"]

    with open("/tmp/data.json", "w") as fout:
        for row in list_of_prods:
            fout.write(json.dumps(row) + "\n")

    run_deployment(name="dbt-command-flow/dbt_downstream_model", timeout=0)


if __name__ == "__main__":
    export()
