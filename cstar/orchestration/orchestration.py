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

_TValue = TypeVar("_TValue")


class Launcher(Protocol, Generic[_THandle]):
    """Contract required to implement a task launcher."""

    @classmethod
    def launch(cls, step: Step, dependencies: list[_THandle]) -> ProcessHandle:
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
    def query_status(cls, step: Step, item: ProcessHandle) -> Status:
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
    def cancel(cls, item: ProcessHandle) -> ProcessHandle:
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

    def __init__(self, step: Step):
        self.step = step
        self.name = step.name
        self._handle: ProcessHandle | None = None
        self.depends_on: list[WorkTask] = []

    @property
    def handle(self) -> ProcessHandle:
        if self._handle is None:
            try:
                # we could hit this when restoring from cache, in which case self.launch will get the cached handle
                # even if we pass None as the launcher
                self._handle = self.launch(launcher=None)
            except:
                raise RuntimeError(
                    "Tried to access handle for {self.name} before it exists, and couldn't restore it from cache."
                )
        return self._handle

    @task(
        persist_result=True, cache_key_fn=cache_func, task_run_name="launch-{self.name}"
    )
    def launch(self, launcher: Launcher):
        clear_working_dir(self.step)  # todo temporary
        handle = launcher.launch(
            self.step, dependencies=[dep.handle for dep in self.depends_on]
        )
        if self._handle is None:
            self._handle = handle
        return handle

    @task(
        persist_result=True, cache_key_fn=cache_func, task_run_name="check-{self.name}"
    )
    def check(self, launcher: Launcher):
        while True:
            status = launcher.query_status(self.step, self.handle)
            print(f"status of {self.name} is {status.name}")
            if Status.is_terminal(status):
                return status
            sleep(15)


def build_and_run(wp_path: Path | str):
    from cstar.orchestration.launch.slurm import SlurmLauncher

    launcher = SlurmLauncher()  # todo, figure out launcher from WP or env

    wp = deserialize(Path(wp_path), Workplan)
    planner = Planner(wp)
    orchestrator = Orchestrator(planner=planner, launcher=launcher)
    orchestrator.run()


class Planner:
    """Identifies depdendencies of a workplan to produce an execution plan."""

    workplan: Workplan
    """The workplan to plan."""

    graph: nx.DiGraph
    """The graph used for task planning."""

    def __init__(
        self,
        workplan: Workplan,
    ) -> None:
        """Initialize the planner and build an execution graph.

        Parameters
        ----------
        workplan: Workplan
            The workplan to be planned.
        """
        self.workplan = workplan

        self._tasks = {}

        self.graph = nx.DiGraph()

        # create all tasks first and add to graph
        for step in workplan.steps:
            print(f"Making task for {step.name}")
            wt = WorkTask(step)
            self._tasks[step.name] = wt
            self.graph.add_node(wt)

        # create worktask deps and graph edges based on wp step dependencies
        for name, t in self._tasks.items():
            for dep in t.step.depends_on:
                t.depends_on.append(self._tasks[dep])
                self.graph.add_edge(self._tasks[dep], t)
            print(f"name: {name}, deps: {t.depends_on}")

        assert nx.is_directed_acyclic_graph(self.graph)

        print(self.graph)
        print([t for t in nx.topological_sort(self.graph)])


class Orchestrator:
    def __init__(self, planner: Planner, launcher: Launcher):
        self.planner = planner
        self.launcher = launcher

    @flow
    def run(self):
        # map step name to prefect futures for submission and check phases
        submissions = {}
        checks = {}

        # submit everything for execution with dependencies
        # use topological sort so that we don't reference dependencies (from submissions dict or trying to get a handle)
        # before they've been submitted.
        # note: we could probably skip nx if we implemented our prefect tasks as transactional, but this is easier
        # for now. see https://docs.prefect.io/v3/advanced/transactions#write-your-first-transaction
        for t in nx.topological_sort(self.planner.graph):
            print(t)
            print(f"launching {t.name}")
            submissions[t.name] = t.launch.submit(
                self.launcher, wait_for=[submissions[dep.name] for dep in t.depends_on]
            )

        # wait for all the slurm submissions to get set up before checking anything
        wait(list(submissions.values()))

        # create check tasks for every task, depending on their real dependencies and their own launch task
        for t in nx.topological_sort(self.planner.graph):
            print(f"adding check task for {t.name} with handle {t.handle}")
            checks[t.name] = t.check.submit(
                self.launcher,
                wait_for=[submissions[t.name]]
                + [checks[dep.name] for dep in t.depends_on],
            )

        # wait on all checks to finish
        wait(list(checks.values()))
