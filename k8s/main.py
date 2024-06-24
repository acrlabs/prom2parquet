#!/usr/bin/env python
import os

import fireconfig as fire
from constructs import Construct
from fireconfig.types import Capability

DAG_FILENAME = "dag.mermaid"
DIFF_FILENAME = "k8s.df"
SERVER_PORT = 1234


class Prom2parquet(fire.AppPackage):
    def __init__(self):
        try:
            with open(os.getenv('BUILD_DIR') + f'/{self.id()}-image') as f:
                image = f.read()
        except FileNotFoundError:
            image = 'PLACEHOLDER'

        env = (fire.EnvBuilder({"AWS_DEFAULT_REGION": "us-east-1"})
            .with_secrets_from("simkube")
        )

        container = fire.ContainerBuilder(
            name=self.id(),
            image=image,
            args=[
                "/prom2parquet",
            ],
        ).with_env(env).with_ports(SERVER_PORT).with_security_context(Capability.DEBUG)

        self._depl = (fire.DeploymentBuilder(app_label=self.id())
            .with_containers(container)
            .with_service()
            .with_node_selector("type", "kind-worker")
        )

    def compile(self, chart: Construct):
        self._depl.build(chart)


if __name__ == "__main__":
    dag_path = f"{os.getenv('BUILD_DIR')}/{DAG_FILENAME}"
    diff_path = f"{os.getenv('BUILD_DIR')}/{DIFF_FILENAME}"
    graph, diff = fire.compile({
        "monitoring": [Prom2parquet()],
    }, dag_path)

    with open(dag_path, "w") as f:
        f.write(graph)

    with open(diff_path, "w") as f:
        f.write(diff)
