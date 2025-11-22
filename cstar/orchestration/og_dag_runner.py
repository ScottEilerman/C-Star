"""
This is a hacky POC and not a production-level solution, it should be removed or heavily improved
before going into develop/main
"""

import os
import shutil
import sys
from pathlib import Path
from time import sleep

import networkx as nx
from prefect import flow, task
from prefect.context import TaskRunContext
from prefect.futures import wait

from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.orchestration import Launcher, ProcessHandle, Status
from cstar.orchestration.serialization import deserialize


def cache_func(context: TaskRunContext, params):
    """Cache on a combination of the task name and user-assigned run id"""
    cache_key = (
        f"{os.getenv('CSTAR_RUNID')}_{params['self'].step.name}_{context.task.name}"
    )
    print(f"Cache check: {cache_key}")
    return cache_key


def clear_working_dir(step: Step):
    # TODO this is temporary only, for my sanity
    _bp = deserialize(Path(step.blueprint), RomsMarblBlueprint)
    out_path = _bp.runtime_params.output_dir
    print(f"clearing {out_path}")
    shutil.rmtree(out_path / "ROMS", ignore_errors=True)
    shutil.rmtree(out_path / "output", ignore_errors=True)
    shutil.rmtree(out_path / "JOINED_OUTPUT", ignore_errors=True)


class WorkTask:
    """Manage execution and monitoring of a step via prefect tasks"""

    def __init__(self, step: Step, launcher: Launcher):
        self.step = step
        self.name = step.name
        self._handle: ProcessHandle = None
        self.launcher = launcher
        self.depends_on: list[WorkTask] = []

    @property
    def handle(self) -> ProcessHandle:
        if self._handle is None:
            print(
                f"no handle exists for {self.name}, likely it was launched previously and cached, or else sequencing has gone bad"
            )
            self._handle = self.launch()
        return self._handle

    @task(
        persist_result=True, cache_key_fn=cache_func, task_run_name="launch-{self.name}"
    )
    def launch(self):
        clear_working_dir(self.step)  # todo temporary
        handle = self.launcher.launch(
            self.step, dependencies=[dep.handle for dep in self.depends_on]
        )
        if self._handle is None:
            self._handle = handle
        return handle

    @task(
        persist_result=True, cache_key_fn=cache_func, task_run_name="check-{self.name}"
    )
    def check(self):
        while True:
            status = self.launcher.query_status(self.step, self.handle)
            print(f"status of {self.name} is {status.name}")
            if Status.is_terminal(status):
                return status
            sleep(15)


@flow
def build_and_run(workplan: Workplan, launcher: SlurmLauncher):
    tasks = {}
    graph = nx.DiGraph()

    # create all tasks first and add to graph
    # collect tasks in dict too just for easy reference
    for step in workplan.steps:
        print(f"Making task for {step.name}")
        wt = WorkTask(step, launcher)
        tasks[step.name] = wt
        graph.add_node(wt)

    # map step dependencies as task dependencies and add graph edges
    for name, t in tasks.items():
        for dep in t.step.depends_on:
            t.depends_on.append(tasks[dep])
            graph.add_edge(tasks[dep], t)
        print(f"name: {name}, deps: {t.depends_on}")

    assert nx.is_directed_acyclic_graph(graph)

    submissions = {}
    checks = {}

    print(graph)
    print([t for t in nx.topological_sort(graph)])

    # submit everything for execution with dependencies
    # use topological sort so that we don't reference dependencies (from submissions dict or trying to get a handle)
    # before they've been submitted.
    # note: we could probably skip nx if we implemented our prefect tasks as transactional, but this is easier
    # for now. see https://docs.prefect.io/v3/advanced/transactions#write-your-first-transaction
    for t in nx.topological_sort(graph):
        print(t)
        print(f"launching {t.name}")
        submissions[t.name] = t.launch.submit(
            wait_for=[submissions[dep.name] for dep in t.depends_on]
        )

    # wait for all the slurm submissions to get set up before checking anything
    wait(list(submissions.values()))

    # create check tasks for every task, depending on their real dependencies and their own launch task
    for t in nx.topological_sort(graph):
        print(f"adding check task for {t.name} with handle {t.handle}")
        checks[t.name] = t.check.submit(
            wait_for=[submissions[t.name]] + [checks[dep.name] for dep in t.depends_on]
        )

    # wait on all checks to finish
    wait(list(checks.values()))


if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    # wp_path = "/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml"
    wp_path = "/home/x-seilerman/wp_testing/workplan.yaml"

    my_run_name = sys.argv[1]
    os.environ["CSTAR_RUNID"] = my_run_name

    workplan = deserialize(Path(wp_path), Workplan)

    build_and_run(workplan, SlurmLauncher())
