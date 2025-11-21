"""
This is a hacky POC and not a production-level solution, it should be removed or heavily improved
before going into develop/main
"""

import os
import shutil
import sys
from pathlib import Path
from time import sleep, time

import networkx as nx
from prefect import flow, task
from prefect.context import TaskRunContext
from prefect.futures import wait

from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import create_scheduler_job, get_status_of_slurm_job
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.orchestration import ProcessHandle, Launcher, Status
from cstar.orchestration.serialization import deserialize

# JobId = str
# JobStatus = str

# these are little mocks you can uncomment if you want to run this locally and not on anvil

# def create_scheduler_job(*args, **kwargs):
#     class dummy:
#         def submit(self):
#             pass
#
#         @property
#         def id(self ):
#             return uuid4()
#
#     return dummy()
#
#
# def get_status_of_slurm_job(*args, **kwargs):
#     sleep(30)
#     return ExecutionStatus.COMPLETED


def cache_func(context: TaskRunContext, params):
    """Cache on a combination of the task name and user-assigned run id"""
    cache_key = f"{os.getenv('CSTAR_RUNID')}_{params['self'].step.name}_{context.task.name}"
    print(f"Cache check: {cache_key}")
    return cache_key

#
# @task(persist_result=True, cache_key_fn=cache_func, log_prints=True)
# def submit_job(step: Step, job_dep_ids: list[str] = []) -> JobId:
#     bp_path = step.blueprint
#     bp = deserialize(Path(bp_path), RomsMarblBlueprint)
#
#     job = create_scheduler_job(
#         commands=f"python3 -m cstar.entrypoint.worker.worker -b {bp_path}",
#         account_key=os.getenv("CSTAR_ACCOUNT_KEY", ""),
#         cpus=bp.cpus_needed,
#         nodes=None,  # let existing logic handle this
#         cpus_per_node=None,  # let existing logic handle this
#         script_path=None,  # puts it in current dir
#         run_path=bp.runtime_params.output_dir,
#         job_name=None,  # to fill with some convention
#         output_file=None,  # to fill with some convention
#         queue_name=os.getenv("CSTAR_QUEUE_NAME"),
#         walltime="00:10:00",  # TODO how to determine this one?
#         depends_on=job_dep_ids,
#     )
#
#     job.submit()
#     print(f"Submitted {step.name} with id {job.id}")
#     return str(job.id)
#
#
# @task(persist_result=True, cache_key_fn=cache_func, log_prints=True)
# def check_job(step, job_id, deps: list[str] = []) -> ExecutionStatus:
#     t_start = time()
#     dur = 10 * 60
#     while time() - t_start < dur:
#         status = get_status_of_slurm_job(job_id)
#         print(f"status of {step.name} is {status}")
#         if status in [
#             ExecutionStatus.CANCELLED,
#             ExecutionStatus.FAILED,
#             ExecutionStatus.COMPLETED,
#         ]:
#             return status
#         sleep(10)
#     return status
#
#
# @flow
# def build_and_run_dag(workplan_path: Path):
#     wp = deserialize(workplan_path, Workplan)
#
#     id_dict = {}
#     status_dict = {}
#
#     no_dep_steps = []
#
#     follow_up_steps = []
#
#     for step in wp.steps:
#         if not step.depends_on:
#             no_dep_steps.append(step)
#         else:
#             follow_up_steps.append(step)
#
#     for step in no_dep_steps:
#         id_dict[step.name] = submit_job(step)
#         status_dict[step.name] = check_job.submit(step, id_dict[step.name])
#
#     while True:
#         for step in follow_up_steps:
#             if all(s in id_dict for s in step.depends_on):
#                 id_dict[step.name] = submit_job(
#                     step, [id_dict[s] for s in step.depends_on]
#                 )
#                 status_dict[step.name] = check_job.submit(
#                     step, id_dict[step.name], [status_dict[s] for s in step.depends_on]
#                 )
#         if len(id_dict) == len(wp.steps):
#             break
#
#     wait(list(status_dict.values()))
#

class WorkTask:
    def __init__(self, step: Step, launcher: Launcher):
        self.step = step
        self.name = step.name
        self._handle = None
        self.launcher = launcher
        self.depends_on: list[WorkTask] = []

    @property
    def handle(self) -> ProcessHandle:
        if self._handle is None:
            print(f"no handle exists for {self.name}, submitting job to establish future")
            self._handle = self.launch()
        return self._handle

    @task(persist_result=True, cache_key_fn=cache_func, task_run_name="launch-{self.name}")
    def launch(self):
        handle = self.launcher.launch(
            self.step, dependencies=[dep.handle for dep in self.depends_on]
        )
        if self._handle is None:
            self._handle = handle
        return handle

    @task(persist_result=True, cache_key_fn=cache_func, task_run_name="check-{self.name}")
    def check(self):
        while True:
            status = self.launcher.query_status(self.step, self.handle)
            print(f"status of {self.name} is {status}")
            if Status.is_terminal(status):
                return status
            sleep(15)

@flow
def build_and_run(workplan: Workplan):
    tasks = {}
    launcher = SlurmLauncher()

    graph = nx.DiGraph()
    # create all tasks first
    for step in workplan.steps:
        print(f"Making task for {step.name}")
        wt = WorkTask(step, launcher)
        tasks[step.name] = wt
        graph.add_node(wt)
    # map step dependencies as task dependencies
    for name, t in tasks.items():
        for dep in t.step.depends_on:
            t.depends_on = [tasks[n] for n in t.step.depends_on]
            graph.add_edge(dep, t)
        print(f"name: {name}, deps: {t.depends_on}")

    assert nx.is_directed_acyclic_graph(graph)

    submissions = {}
    checks = {}

    # submit everything for execution with dependencies
    for t in nx.topological_sort(graph):
        print(f"launching {t.name}")
        submissions[t.name] = t.launch.submit(wait_for = [submissions[dep.name] for dep in t.depend_on]) #wait_for= [dep.handle for dep in t.depends_on]

    wait(*submissions.values())
    # create check tasks for every task, depending on their real dependencies and their
    # own launch task

    for t in nx.topological_sort(graph):
        print(f"checking {t.name} with handle {t.handle}")

        checks[t.name] = t.check.submit(wait_for = [submissions[t.name]] + [checks[dep.name] for dep in t.depend_on]) #wait_for = [dep.handle for dep in t.depends_on] + [t.handle]

    # all_tasks = [*submissions.values(), *checks.values()]
    # print(all_tasks)
    wait(*checks.values())

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
    for bp in [s.blueprint for s in workplan.steps]:
        _bp = deserialize(Path(bp), RomsMarblBlueprint)
        out_path = _bp.runtime_params.output_dir
        print(out_path)
        shutil.rmtree(out_path / "ROMS", ignore_errors=True)
        shutil.rmtree(out_path / "output", ignore_errors=True)
        shutil.rmtree(out_path / "JOINED_OUTPUT", ignore_errors=True)

    build_and_run(workplan)

