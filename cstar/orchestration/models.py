import logging
import typing as t
from copy import deepcopy
from datetime import datetime, timezone
from enum import StrEnum, auto
from pathlib import Path, PosixPath

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    PlainSerializer,
    PositiveInt,
    StringConstraints,
    ValidationInfo,
    WithJsonSchema,
    field_validator,
    model_validator,
)
from pytimeparse import parse

from cstar.base.log import get_logger

# todo no debug logging
logger = get_logger(__name__, level=logging.DEBUG)

RequiredString: t.TypeAlias = t.Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
"""A non-empty string with no leading or trailing whitespace."""

KeyValueStore: t.TypeAlias = dict[str, str | float | list[str] | list[float]]
"""A collection of user-defined key-value pairs."""

TargetDirectoryPath = t.Annotated[
    Path,
    PlainSerializer(lambda p: str(p), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
"""Path to a directory that may not exist until runtime."""


class ConfiguredBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

class HashableFile(ConfiguredBaseModel):
    location: str
    hash: str | None = Field(default=None, init=False, validate_default=False)


class DocLocMixin(ConfiguredBaseModel):
    documentation: str = Field(default="", validate_default=False)
    """Description of input data provenance; used in provenance roll-up."""

    locked: bool = Field(default=False, init=False, frozen=True)
    """Mutability of the parameter set."""

class SingleFileDataset(HashableFile, DocLocMixin):
    pass


class MultiFileDataset(DocLocMixin, ConfiguredBaseModel):
    files: list[HashableFile] = Field(default_factory=list)


#
# class PathDatasource(ConfiguredBaseModel):
#     category: t.Literal["path"] = "path"
#     """The datasource category used as a type discriminator."""
#
#     location: list[FilePath | DirectoryPath | HttpUrl]
#     """The path to the file or directory containg data."""


class PathFilter(ConfiguredBaseModel):
    """A filter used to specify a subset of files."""

    category: t.Literal["path-filter"] = "path-filter"

    directory: str | None = Field(default="", validate_default=False)
    """Subdirectory that should be searched or kept."""

    files: list[str] = Field(default_factory=list, validate_default=False)
    """List of specific file names that must be kept.

    File name filtering is combined with the directory filter, if one
    is provided."""


class ForcingConfiguration(ConfiguredBaseModel):
    """Configuration of the forcing parameters of the model."""

    boundary: MultiFileDataset
    """Boundary forcing."""

    surface: MultiFileDataset
    """Surface forcing"""

    corrections: MultiFileDataset | None = Field(default=None, validate_default=False)
    """Wind or other forcing corrections."""

    tidal: SingleFileDataset | None = Field(default=None, validate_default=False)
    """Tidal forcing."""

    river: SingleFileDataset | None = Field(default=None, validate_default=False)
    """River forcing."""


class CodeRepository(DocLocMixin, ConfiguredBaseModel):
    """Reference to a remote code repository with optional path filtering
    and point-in-time specification.
    """

    location: HttpUrl | str
    """Location of the remote code repository."""

    commit: str = Field(default="", min_length=1, validate_default=False)
    """A specific commit to be used."""

    branch: str = Field(default="", min_length=1, validate_default=False)
    """A specific branch to be used."""

    filter: PathFilter | None = Field(default=None, validate_default=False)
    """A filter specifying the files to be retrieved and persisted from the repository."""

    @property
    def checkout_target(self) -> str:
        return self.commit or self.branch

    @model_validator(mode="after")
    def _model_validator(self) -> "CodeRepository":
        """Perform validation on the model after field-level validation is complete.

        - Ensure that one of commit or branch checkout target is supplied
        """
        if self.commit and self.branch:
            msg = "Supply only one of commit hash or branch."
            raise ValueError(msg)

        if not self.commit and not self.branch:
            msg = "Either the commit hash or branch must be supplied."
            raise ValueError(msg)

        return self


class ROMSCompositeCodeRepository(ConfiguredBaseModel):
    """Collection of repositories used to build, configure, and execute ROMS."""

    roms: CodeRepository
    """The baseline ROMS repository."""

    run_time: CodeRepository
    """Codebase used to modify the runtime behavior of ROMS."""

    compile_time: CodeRepository
    """Codebase used to modify base ROMS compilation."""

    marbl: CodeRepository | None = Field(default=None, validate_default=False)
    """Codebase used to add MARBL to the simulation."""


class BlueprintState(StrEnum):
    """The allowed states for a work plan."""

    # TODO: determine if unique states for Bp/WP are being considered, if not. discard.
    NotSet = auto()
    """Default, unset value."""

    Draft = auto()
    """A blueprint that has not been validated."""

    Validated = auto()
    """A blueprint that has been validated."""


class Application(StrEnum):
    """The supported application types."""

    ROMS = auto()
    """A UCLA-ROMS simulation that will not make use of biogeochemical data."""
    ROMS_MARBL = auto()
    """A UCLA-ROMS simulation coupled with a MARBL biogeochemical component."""
    HOSTNAME = auto()
    """A call to the hostname executable to simplify testing."""





class ParameterSet(DocLocMixin, ConfiguredBaseModel):


    # TODO: 1. Consider an empty string default hash to avoid nulls
    # TODO: 2. Consider another model so the draft version doesn't have a hash.
    hash: str | None = Field(default=None, init=False, validate_default=False)
    """Hash used to verify the parameters are unchanged."""

    # NOTE: this doesn't support parameters without values (e.g. --no-truncate)
    # model_config: t.ClassVar[ConfigDict] = ConfigDict(extra="allow")
    # """Enable the model to parse user-defined attributes."""

    @model_validator(mode="after")
    def _model_validator(self) -> "ParameterSet":
        """Perform validation on the model after field-level validation is complete.

        Ensure the dynamically added parameters meet the minimum naming standard.
        """
        if self.locked and not self.hash:
            msg = "A locked parameter set must include a hash"
            raise ValueError(msg)
        # if self.model_extra:
        #     for k, v in self.model_extra.items():
        #         stripped = k.strip()
        #
        #         logger.debug(f"Handling user-specified extra param {k} with value {v}")
        #
        #         if " " in stripped or not stripped:
        #             msg = f"Parameter name `{k}` cannot include whitespace"
        #             raise ValueError(msg)
        #         if not v.strip():
        #             msg = f"Parameter `{k}` does not have a value"
        #             raise ValueError(msg)
        #
        #         # re-write dynamic property keys without leading or trailing whitespace
        #         if " " in k:
        #             # TODO: go see if this actually works in a test or if delattr is required
        #             self.model_extra.pop(k)
        #             self.model_extra.update({stripped: v})

        return self


class RuntimeParameterSet(ParameterSet):
    """Parameters for the execution of the model.

    Supports user-defined attributes to enable customization.
    """

    start_date: datetime = datetime(1, 1, 1, tzinfo=timezone.utc)
    """Start of data time range to be used in the simulation."""

    end_date: datetime = datetime(2, 1, 1, tzinfo=timezone.utc)
    """End of data time range to be used in the simulation."""

    # restart_freq: str
    checkpoint_frequency: str = Field(
        default="1d",
        min_length=2,
        pattern="(?P<scalar>[1-9][0-9]*)(?P<unit>[hdwmy])",
    )
    """Time period allowed between creation of a checkpoint file.

    Supply a string representing the desired time period, such as:
    - every day: "1d"
    - every week: "1w"
    - every month: "1m" or "4w" or "31d"
    - every 2.5 days: "2d 12h" or "60h"

    A short time period will reduce data re-processing upon restart at the cost
    of additional disk usage and compute used for checkpointing.
    """

    output_dir: TargetDirectoryPath = Path()
    """Directory where runtime outputs will be stored."""

    @field_validator("checkpoint_frequency", mode="after")
    @classmethod
    def _validate_checkpoint_frequency(
        cls,
        value: str,
        _info: ValidationInfo,
    ) -> str:
        """Verify a valid range string for the checkpoint frequency was supplied.

        Parameters:
            value : str
                The value of the checkpoint frequency property
            _info : ValidationInfo
                Metadata for the current validation context
        """
        if not parse(value):
            msg = "Invalid checkpoint frequency supplied."
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _model_validator(self) -> "RuntimeParameterSet":
        """Perform validation on the model after field-level validation is complete."""
        if self.end_date <= self.start_date:
            msg = "start_date must precede end_date"
            raise ValueError(msg)

        return self


class PartitioningParameterSet(ParameterSet):
    """Parameters for the partitioning of the model.

    Supports user-defined attributes to enable customization.
    """

    # todo: adding defaults to consume a 128 core node. consider removing.
    n_procs_x: PositiveInt = 16
    """Number of processes used to subdivide the domain on the x-axis."""

    # todo: adding defaults to consume a 128 core node. consider removing.
    n_procs_y: PositiveInt = 8
    """Number of processes used to subdivide the domain on the y-axis."""


class ModelParameterSet(ParameterSet):
    time_step: PositiveInt


class Blueprint(ConfiguredBaseModel):
    name: RequiredString
    """A unique, user-friendly name for this blueprint."""

    description: RequiredString
    """A user-friendly description of the scenario to be executed by the blueprint."""

    application: Application = Application.ROMS
    """The process type to be executed by the blueprint."""

    state: BlueprintState = BlueprintState.NotSet
    """The current validation status of the blueprint."""

    valid_start_date: datetime
    """Beginning of the time range for the available data."""

    valid_end_date: datetime
    """End of the time range for the available data."""

    code: ROMSCompositeCodeRepository
    """Code repositories used to build, configure, and execute the ROMS simulation."""

    initial_conditions: SingleFileDataset

    grid: SingleFileDataset

    forcing: ForcingConfiguration
    """Forcing configuration."""

    partitioning: PartitioningParameterSet
    """User-defined partitioning parameters."""

    model_params: ModelParameterSet
    """User-defined model parameters."""

    # should this be nullable instead of a union with a default?
    runtime_params: RuntimeParameterSet = RuntimeParameterSet()
    """User-defined runtime parameters."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _model_validator(self) -> "Blueprint":
        """Perform validation on the model after field-level validation is complete."""
        if self.valid_end_date <= self.valid_start_date:
            msg = "valid_start_date must precede valid_end_date"
            raise ValueError(msg)

        return self


class WorkplanState(StrEnum):
    """The allowed states for a work plan."""

    NotSet = auto()

    Draft = auto()
    """A workflow that has not been validated."""

    Validated = auto()
    """A workflow that has been validated."""


class ComputePlatform(StrEnum):
    """Supported execution platforms."""

    Local = auto()
    """Indicate that execution should take place locally."""

    AWS = auto()
    """Indicate that execution should take place on AWS resources."""

    Perlmutter = auto()
    """Indicate that execution should take place on Perlmutter."""


class Step(BaseModel):
    """An individual unit of execution within a workplan."""

    name: RequiredString
    """The user-friendly name of the step."""

    application: RequiredString
    """The user-friendly name of the application executed in the step."""

    blueprint: FilePath
    """The blueprint that will be executed in this step."""

    depends_on: list[RequiredString] = Field(
        default_factory=list,
        frozen=True,
    )
    """An optional list of external step names that must execute prior to this step.

    Cycles are not permitted.
    """

    blueprint_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for blueprint attributes."""

    compute_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for compute attributes."""

    workflow_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for workflow attributes."""

    # @computed_field
    # @property
    # def slug(self) -> str:
    #     """Return a URL safe version of the step name."""
    #     return re.sub(r"\s+", "-", self.name.strip())

    # @computed_field
    # @property
    # def depends_on_slugs(self) -> t.Sequence[str]:
    #     return [re.sub(r"\s+", "-", step.strip()) for step in self.depends_on]


class Workplan(BaseModel):
    """A collection of executable steps and the associated configuration to run them."""

    name: RequiredString
    """The user-friendly name of the workplan."""

    description: RequiredString
    """A user-friendly description of the workplan."""

    steps: t.Sequence[Step] = Field(
        default_factory=list,
        min_length=1,
        frozen=True,
    )
    """The steps to be executed by the workplan."""

    state: WorkplanState = Field(default=WorkplanState.NotSet)
    """The current validation status of the workplan."""

    compute_environment: KeyValueStore = Field(
        default_factory=dict,
        frozen=True,
    )
    """A collection of key-value pairs specifying attributes for the target compute environment."""

    runtime_vars: list[str] = Field(
        default_factory=list,
        validate_default=False,
        frozen=True,
    )
    """A collection of user-defined variables that will be populated at runtime."""

    @field_validator("steps", mode="before")
    @classmethod
    def _deep_copy_steps(cls, value: list[Step]) -> list[Step]:
        """Ensure the steps provided are deep copied to avoid external change propagation.

        Parameters
        ----------
        value : list[Step]
            The list of steps assigned to the instance

        Returns
        -------
        list[Step]
            The deep-copied step list

        """
        return deepcopy(value)

    @field_validator("runtime_vars", mode="after")
    @classmethod
    def _check_runtime_vars(cls, value: list[str]) -> list[str]:
        """Ensure no duplicate runtime vars are passed.

        Parameters
        ----------
        value : list[str]
            Variable names used at runtime

        """
        var_counter = t.Counter(value)
        most_common = var_counter.most_common(1)
        var_name, var_count = most_common[0] if most_common else ("", 0)

        if var_count > 1:
            msg = f"Duplicate runtime variables provided: {var_count} copies of {var_name}"
            raise ValueError(msg)

        return value

    @field_validator("steps", mode="after")
    @classmethod
    def _check_steps(cls, value: list[Step]) -> list[Step]:
        """Xxx.

        Parameters
        ----------
        value : list[Step]
            Variable names used at runtime

        """
        name_counter = t.Counter(step.name for step in value)
        most_common = name_counter.most_common(1)
        step_name, step_count = most_common[0]
        step_name, step_count = most_common[0] if most_common else ("", 0)

        if step_count > 1:
            msg = f"Step names must be unique. Found {step_count} steps with name {step_name}"
            raise ValueError(msg)

        return value

    @model_validator(mode="after")
    def _model_validator(self) -> "Workplan":
        """Validate attribute relationships."""
        return self
