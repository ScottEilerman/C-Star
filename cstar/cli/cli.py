import typer

import cstar.cli.blueprint as bp
import cstar.cli.template as tmpl
import cstar.cli.workplan as wp

app = typer.Typer()
app.add_typer(bp.app, name="blueprint")
app.add_typer(wp.app, name="workplan")
app.add_typer(tmpl.app, name="template")

if __name__ == "__main__":
    app()
