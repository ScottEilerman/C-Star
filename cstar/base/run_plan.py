from enum import StrEnum

from cstar.base.common import ConfiguredBaseModel

class LauncherEnum(StrEnum):
    PERLMUTTER_LOCAL = "perlmutter_local"
    SFAPI = "sfapi"
    SUBPROCESS = "subprocess"

class Step(ConfiguredBaseModel):
    name: str
    command: dict
    depends_on: list[str]
    blueprint: str | None = None
    segment_length: str | None = None

class RunPlan(ConfiguredBaseModel):
    name: str
    launcher: LauncherEnum
    description: str = ""
    shareable: bool = True
    runtime_vars: list[str]
    default_blueprint: str | None = None
    default_segment_length: str | None = None

    steps: list[Step]