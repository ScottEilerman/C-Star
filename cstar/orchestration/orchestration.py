import os
import shutil
from enum import IntEnum, auto
from pathlib import Path
from time import sleep
from typing import Generic, Protocol, TypeVar

import networkx as nx
from prefect import flow, task
from prefect.context import TaskRunContext
from prefect.futures import wait

from cstar.orchestration.models import RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.serialization import deserialize


class ProcessHandle:
    """Contract used to identify processes created by any launcher."""

    pid: str
    """The process identifier."""

    def __init__(self, pid: str) -> None:
        """Initialize the handle.

        Parameters
        ----------
        pid : str
            A unique ID identifying a running process.
        """
        self.pid = pid


class Status(IntEnum):
    """The state of a running task."""

    Unsubmitted = auto()
    """A task that has not been submitted by a launcher."""
    Submitted = auto()
    """A task that has been submitted by a launcher and awaits a status update."""
    Running = auto()
    """A task that was submitted and has not terminated."""
    Ending = auto()
    """A task that has reported itself as nearing completion."""
    Done = auto()
    """A task that has terminated without error."""
    Cancelled = auto()
    """A task terminated due to cancellation (by the user or system)."""
    Failed = auto()
    """A task that terminated due to some failure in the task."""

    @classmethod
    def is_terminal(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of terminal statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Done, Status.Cancelled, Status.Failed}

    @classmethod
    def is_failure(cls, status) -> bool:
        """Return `True` if a status is in the set of terminal statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Cancelled, Status.Failed}

    @classmethod
    def is_running(cls, status) -> bool:
        """Return `True` if a status is in the set of in-progress statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Submitted, Status.Running, Status.Ending}


_THandle = TypeVar("_THandle", bound=ProcessHandle)


class Task(Generic[_THandle]):
    """A task represents a live-execution of a step."""

    status: Status
    """Current task status."""

    step: Step
    """The step containing task configuration."""

    handle: _THandle
    """The unique process identifier for the task."""

    def __init__(
        self,
        step: Step,
        handle: _THandle,
        status: Status = Status.Unsubmitted,
    ) -> None:
        """Initialize the Task record.

        Parameters
        ----------
        step : Step
            The workplan `Step` that triggered the task to run
        handle : _THandle
            A handle that used to identify the running task.
        status : Status
            The current status of the task
        """
        self.status = status
        self.step = step
        self.handle = handle


_TValue = TypeVar("_TValue")


class Launcher(Protocol, Generic[_THandle]):
    """Contract required to implement a task launcher."""

    @classmethod
    def launch(cls, step: Step, dependencies: list[_THandle]) -> Task[_THandle]:
        """Launch a process for a step.

        Parameters
        ----------
        step : Step
            The step to launch

        Returns
        -------
        Task[_THandle]
            The newly launched task.
        """
        ...

    @classmethod
    def query_status(cls, step: Step, item: Task[_THandle] | _THandle) -> Status:
        """Retrieve the current status for a running task.

        Parameters
        ----------
        item : Task[_THandle] or ProcessHandle
            A task or process handle to query for status updates.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...

    @classmethod
    def cancel(cls, item: Task[_THandle]) -> Task[_THandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task[_THandle] or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...


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
        self._handle: ProcessHandle | None = None
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
def build_and_run(wp_path: Path | str):
    tasks = {}
    graph = nx.DiGraph()

    from cstar.orchestration.launch.slurm import SlurmLauncher

    launcher = SlurmLauncher()  # todo, figure out launcher from WP or env

    workplan = deserialize(Path(wp_path), Workplan)

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
