import os
from pathlib import Path
from typing import Annotated

import typer

from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import build_and_run
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def run(path: Path, name: str) -> None:
    """
    Run the workplan using an ephemeral prefect server

    Parameters
    ----------
    path: Path to the workplan
    name: Unique run-id (use a previous one to restore cached steps)

    """
    # TODO: load from ~/.cstar/config (e.g. cstar config init)
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    os.environ["CSTAR_RUNID"] = name

    build_and_run(path)


@app.command()
def check(
    path: Annotated[Path, typer.Argument(help="The path to the workplan")],
) -> None:
    """Check workplan validity"""
    try:
        model = deserialize(path, Workplan)
        assert model, "Model was not deserialized"
        print(f"workplan {path} is valid")
    except ValueError as ex:
        print(f"Error occurred: {ex}")


if __name__ == "__main__":
    app()
