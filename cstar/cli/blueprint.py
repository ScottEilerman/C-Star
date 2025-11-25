import asyncio
from pathlib import Path

import typer

from cstar.entrypoint import SimulationRunner
from cstar.entrypoint.service import ServiceConfiguration
from cstar.entrypoint.worker.worker import BlueprintRequest, JobConfig
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(path: Path) -> None:
    """The action handler for the workplan-check."""
    try:
        model = deserialize(path, RomsMarblBlueprint)
        assert model, "Blueprint was not deserialized"
        print(f"blueprint {path} is valid")
    except ValueError as ex:
        print(f"Error occurred: {ex}")


def configure_simulation_runner(path: Path) -> SimulationRunner:
    """Create a `SimulationRunner` to execute the blueprint.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute

    Returns
    -------
    SimulationRunner
        A simulation runner configured to execute the blueprint
    """
    account_id = "m4632"
    walltime = "48:00:00"

    request = BlueprintRequest(path.as_posix())
    service_config = ServiceConfiguration(
        loop_delay=0, health_check_frequency=300, health_check_log_threshold=25
    )
    job_config = JobConfig(account_id, walltime)

    return SimulationRunner(request, service_config, job_config)


@app.command()
def run(path: Path) -> None:
    """Execute a blueprint synchronously using a worker service.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    print("Executing blueprint via blocking worker service.")
    runner = configure_simulation_runner(path)
    asyncio.run(runner.execute())


if __name__ == "__main__":
    app()
