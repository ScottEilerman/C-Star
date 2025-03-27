from abc import ABC, abstractmethod
from enum import StrEnum

from cstar.base.blueprint import Blueprint, read_blueprint, get_application
from cstar.base.common import ConfiguredBaseModel

class LauncherEnum(StrEnum):
    PERLMUTTER_LOCAL = "perlmutter_local"
    SFAPI = "sfapi"
    SUBPROCESS = "subprocess"

class RunStepModel(ConfiguredBaseModel):
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

    steps: list[RunStepModel]

class Status(StrEnum):
    SUBMITTED = ""

class Launcher(ABC):

    @abstractmethod
    def submit_job(self, payload) -> Status:
        pass

    @abstractmethod
    def check_job_status(self, job_id) -> Status:
        pass


class RunStep:
    def __init__(self, model: RunStepModel):
        self.model = model
        self.parsed_blueprint = read_blueprint(self.model.blueprint)
        self.application = get_application(self.parsed_blueprint.application)

    # @property
    # def application(self):
    #     return self.parsed_blueprint.application

    @property
    def payload(self):
        return {}

    def submit(self, launcher: Launcher) -> Status:
        return launcher.submit_job(self.payload)


