import datetime
import typing as t
from pathlib import Path
from unittest.mock import MagicMock

import networkx as nx
import pytest

from cstar.orchestration.models import (
    Application,
    Blueprint,
    BlueprintState,
    CodeRepository,
    ForcingConfiguration,
    HashableFile,
    ModelParameterSet,
    MultiFileDataset,
    PartitioningParameterSet,
    ROMSCompositeCodeRepository,
    RuntimeParameterSet,
    SingleFileDataset,
    Workplan,
)
from cstar.orchestration.orchestrator import GraphPlanner, SerialPlanner


@pytest.fixture
def test_graph() -> nx.DiGraph:
    dependency_map = {
        "task-1": [],
        "task-2": [],
        "task-3": [],
        "task-4": [],
        "task-5": ["task-3", "task-4"],
        "task-6": ["task-1", "task-2", "task-5"],
        "task-7": ["task-5"],
    }

    edges: list[tuple[int | str, int | str]] = []
    for key, predecessor_list in dependency_map.items():
        edges.extend((predecessor, key) for predecessor in predecessor_list)

    return nx.DiGraph(edges, name="test-graph")


def test_build_graph(tmp_path: Path, test_graph: nx.DiGraph) -> None:
    """Verify that the orchestrator renders an image correctly."""
    mock_plan = MagicMock(spec=Workplan)
    mock_plan.configure_mock(name="mock plan")

    planner = GraphPlanner(workplan=mock_plan, graph=test_graph)

    image_path = planner.render(tmp_path)

    assert image_path.exists()


def test_serial_planner(tmp_path: Path, test_graph: nx.DiGraph) -> None:
    """Verify that a serialized plan is produced."""
    mock_plan = MagicMock(spec=Workplan)
    mock_plan.configure_mock(name="mock plan")

    if True:
        tmp_path = Path()
    planner = SerialPlanner(workplan=mock_plan, graph=test_graph, artifact_dir=tmp_path)

    # should be one item in the plan for every node and must omit the start node
    assert planner.plan == [
        "task-1",
        "task-2",
        "task-3",
        "task-4",
        "task-6",
        "task-5",
        "task-7",
    ]


# @pytest.mark.skip(reason="Used for development purposes, only")
def test_make_a_minimum_blueprint_yaml(
    tmp_path: Path,
    serialize_blueprint: t.Callable[[Blueprint, Path], str],
) -> None:
    """Use a unit test to create a blueprint YAML doc instead of doing so by hand..."""
    bp_path = tmp_path / "blueprint.yml"
    random_file = tmp_path / "random_file.nc"
    random_file.touch()

    blueprint = Blueprint(
        name="Test Blueprint Name",
        description="This is the description of my test blueprint",
        application=Application.HOSTNAME,
        state=BlueprintState.Draft,
        valid_start_date=datetime.datetime(2020, 1, 1, 0, 0, 0),
        valid_end_date=datetime.datetime(2020, 2, 1, 0, 0, 0),
        code=ROMSCompositeCodeRepository(
            roms=CodeRepository(
                location="http://github.com/ankona/ucla-roms",
                branch="main",
            ),
            run_time=CodeRepository(
                location="http://github.com/ankona/ucla-roms",
                branch="main",
            ),
            compile_time=CodeRepository(
                location="http://github.com/ankona/ucla-roms",
                branch="main",
            ),
            marbl=None,
        ),
        forcing=ForcingConfiguration(
            boundary=MultiFileDataset(files=[HashableFile(location=random_file)]),
            surface=MultiFileDataset(files=[HashableFile(location=random_file)]),
            corrections=MultiFileDataset(files=[HashableFile(location=random_file)]),
            tidal=SingleFileDataset(location=random_file),
            river=SingleFileDataset(location=random_file),
        ),
        partitioning=PartitioningParameterSet(n_procs_x=1, n_procs_y=2),
        model_params=ModelParameterSet(time_step=1),
        runtime_params=RuntimeParameterSet(),
        grid=SingleFileDataset(location=random_file),
        initial_conditions=SingleFileDataset(location=random_file),
    )

    bp_yaml = serialize_blueprint(blueprint, bp_path)

    assert bp_yaml.strip()
    assert bp_path.exists()
