import typer

import cstar.cli.blueprint as bp
import cstar.cli.workplan as wp

app = typer.Typer()
app.add_typer(bp.app, name="blueprint")
app.add_typer(wp.app, name="workplan")

if __name__ == "__main__":
    app()
