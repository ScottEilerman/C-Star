from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Self, Annotated, Optional, Any, ClassVar
from urllib.parse import urlparse, urljoin

import dateutil
import pooch
import requests
from pydantic import BaseModel, Field, PlainValidator, WithJsonSchema, model_validator, computed_field
import yaml
from enum import StrEnum
import pandas as pd
import datetime as dt

from cstar.base.utils import _get_sha256_hash

from cstar.base.common import ConfiguredBaseModel



# see https://github.com/pydantic/pydantic/discussions/6972
_Timestamp = Annotated[
    pd.Timestamp,
    PlainValidator(lambda x: pd.Timestamp(x)),
    WithJsonSchema({"type": 'date-time'})
]

class CodeRepo(ConfiguredBaseModel):
    source_repo: str
    subdir: str | None = None
    checkout_target: str = "main"
    files: list[str] = []



class InputDatasetBase(ConfiguredBaseModel):
    # define fields that all input datasets have; for now, just one, origin
    origin: str = ""  # description of input data provenance to be included in provenance roll-up

class RunPlanArtifactDataSource(InputDatasetBase):
    run_plan: str | None # name of run plan; null = the current run plan
    run_id: str | None  # specific run of that run plan; null = the currently executing run
    step: str  # name of the step within the run plan to pluck output from
    files: list[str] # file paths within the step's output to reference

class LocationDataSource(InputDatasetBase):
    location: str | list[str] | Path | list[Path]
    file_hash: str | None = None # must be specified if location is specified and remote

    # I don't think these belong on here, but trying to get parity with the old class for the moment
    start_date: _Timestamp | None = None
    end_date: _Timestamp | None = None

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if isinstance(self.location, Path):
            self.location = str(self.location)
        elif isinstance(self.location, list):
            self.location = [str(p) for p in self.location]

    @computed_field
    @property
    def location_type(self) -> str:
        """Get the location type (e.g. "path" or "url") from the "location"
        attribute."""
        urlparsed_location = urlparse(str(self.location))
        if all([urlparsed_location.scheme, urlparsed_location.netloc]):
            return "url"
        elif Path(self.location).expanduser().exists():
            return "path"
        else:
            raise ValueError(
                f"{self.location} is not a recognised URL or local path pointing to an existing file or directory"
            )

    @computed_field
    @property
    def source_type(self) -> str:
        """Get the source type (e.g. "netcdf") from the "location" attribute."""
        loc = Path(self.location).expanduser()

        if (loc.suffix.lower() == ".git") or ((loc / ".git").is_dir()):
            # TODO: a remote repository might not have a .git suffix, more advanced handling needed
            return "repository"
        elif loc.is_dir():
            return "directory"
        elif loc.suffix.lower() in {".yaml", ".yml"}:
            return "yaml"
        elif loc.suffix.lower() == ".nc":
            return "netcdf"
        else:
            raise ValueError(f"{Path(self.location)} does not exist or is not a supported file type")

    @computed_field
    @property
    def basename(self) -> str:
        """Get the basename (typically a file name) from the location attribute."""
        return Path(self.location).name

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\n location: {self.location}"
        if self.file_hash is not None:
            base_str += f"\n file hash: {self.file_hash}"
        base_str += f"\n basename: {self.basename}"
        base_str += f"\n location type: {self.location_type}"
        base_str += f"\n source type: {self.source_type}"
        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}(location={self.location!r}"
        if self.file_hash is not None:
            repr_str += f", file_hash={self.file_hash!r}"
        repr_str += ")"

        return repr_str


class ParamDataSource(InputDatasetBase):
    params: dict # can probably use a base class of sorts later

# define type shorthand:
InputDatasetType = RunPlanArtifactDataSource | LocationDataSource | ParamDataSource

class Discretization(ConfiguredBaseModel):
    time_step: int = Field(gt=0)
    n_procs_x: int = Field(gt=0)
    n_procs_y: int = Field(gt=0)

class ApplicationEnum(StrEnum):
    ROMS_MARBL = "roms_marbl"
    ROMS_TOOLS = "roms_tools"

class Blueprint(ConfiguredBaseModel, ABC):
    name: str
    description: str = ""
    application: ApplicationEnum

    valid_start_date: _Timestamp | None
    valid_end_date: _Timestamp | None
    discretization: Discretization | None


def read_blueprint(yaml_path: str | Path) -> Blueprint:
    class _GenericBlueprint(Blueprint):
        # just letting us read the basic attributes with pydantic,
        # despite ABC
        pass

    generic = _GenericBlueprint.parse_file(yaml_path)
    application = get_application(generic.application)
    bp_schema = application.blueprint_schema
    return bp_schema.parse_file(yaml_path)


class RomsMarblBlueprint(Blueprint):
    codebase: CodeRepo
    runtime_code: CodeRepo
    compile_time_code: CodeRepo
    marbl_codebase: CodeRepo

    model_grid: InputDatasetType
    initial_conditions: InputDatasetType
    tidal_forcing: InputDatasetType
    surface_forcing: list[InputDatasetType]
    boundary_forcing: list[InputDatasetType]

# @dataclass
class Application(ABC):
    name: ClassVar

    command_params: ConfiguredBaseModel # valid command params in the run step
    blueprint_schema: Blueprint
    cli_entrypoint: str # no clue what we need here yet

class RomsMarblCommands(ConfiguredBaseModel):
    start_date: str
    end_date: str
    restart_file: str
    time_step: int

class RomsMarblApplication(Application):
    name = "roms_marbl"
    command_params = RomsMarblCommands
    blueprint_schema = RomsMarblBlueprint
    cli_entrypoint = "python3 run_cstar.py --roms_marbl"



APPLICATION_MAP = {
    ApplicationEnum.ROMS_MARBL: RomsMarblApplication,
    ApplicationEnum.ROMS_TOOLS: NotImplementedError()
}

def get_application(application_name: ApplicationEnum) -> Application:
    return APPLICATION_MAP[application_name]()
# class InputDataset(LocationDataSource):
#     """Describes spatiotemporal data needed to run a unique instance of a model
#     simulation.
#
#     Attributes:
#     -----------
#     source: DataSource
#         Describes the location and type of the source data
#     file_hash: str, default None
#         The 256 bit SHA sum associated with a (remote) file for verifying downloads
#     working_path: Path or list of Paths, default None
#         The path(s) where the input dataset is being worked with locally, set when `get()` is called.
#
#     Methods:
#     --------
#     get(local_dir)
#         Fetch the file containing this input dataset and save it to `local_dir`
#     """
#
#     def __init__(
#         self, **kwargs
#     ):
#         """Initialize an InputDataset object associated with a model simulation using a
#         source URL and file hash.
#
#         Parameters:
#         -----------
#         location: str
#             URL or path pointing to a file either containing this dataset or instructions for creating it.
#             Used to set the `source` attribute.
#         file_hash: str, optional
#             The 256 bit SHA sum associated with the file for verification if remote
#         """
#         super().__init__(**kwargs)
#         if (
#             (self.location_type == "url")
#             and (self.file_hash is None)
#             and (self.source_type != "yaml")
#         ):
#             raise ValueError(
#                 f"Cannot create InputDataset for \n {self.location}:\n "
#                 + "InputDataset.source.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
#                 + "A file hash is required to verify non-plaintext files downloaded from remote sources."
#             )
#
#         # Initialize object state:
#         self.working_path: Optional[Path | list[Path]] = None
#         self._local_file_hash_cache: Optional[dict] = None
#         self._local_file_stat_cache: Optional[dict] = None
#     #
#     # @model_validator(mode='after')
#     # def add_extra_params(self) -> Self:
#     #
#     #     if (
#     #             (self.location_type == "url")
#     #             and (self.file_hash is None)
#     #             and (self.source_type != "yaml")
#     #     ):
#     #         raise ValueError(
#     #             f"Cannot create InputDataset for \n {self.location}:\n "
#     #             + "InputDataset.source.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
#     #             + "A file hash is required to verify non-plaintext files downloaded from remote sources."
#     #         )
#     #
#     #     # Initialize object state:
#     #     self.working_path: Optional[Path | list[Path]] = None
#     #     self._local_file_hash_cache: Optional[dict] = None
#     #     self._local_file_stat_cache: Optional[dict] = None
#     #     return self
#
#     @property
#     def exists_locally(self) -> bool:
#         """Check if this InputDataset exists on the local filesystem.
#
#         This method verifies the following for each file in `InputDataset.working_path`:
#         1. The file exists at the specified path.
#         2. The file's current size and modification date match values cached by `InputDataset.get()`
#         3. If the size matches but the modification time does not, the file's SHA-256 hash
#            is computed and compared against a value cached by `InputDataset.get()`
#
#         Returns:
#         --------
#         exists_locally (bool): True if all files pass the existence, size, modification
#             time, and (if necessary) hash checks. Returns False otherwise.
#
#         Notes:
#         ------
#             If C-Star cannot access cached file statistics, it is impossible to verify
#             whether the InputDataset is correct, and so `False` is returned.
#         """
#         if (self.working_path is None) or (self._local_file_stat_cache is None):
#             return False
#
#         # Ensure working_path is a list for unified iteration
#         paths = (
#             self.working_path
#             if isinstance(self.working_path, list)
#             else [self.working_path]
#         )
#
#         for path in paths:
#             # Check if the file exists
#             if not path.exists():
#                 return False
#
#             # Retrieve the cached stats
#             cached_stats = self._local_file_stat_cache.get(path)
#             if cached_stats is None:
#                 return False  # No stats cached for this file
#
#             # Compare size first
#             current_stats = path.stat()
#             if current_stats.st_size != cached_stats.st_size:
#                 return False  # Size mismatch, no need to check further
#
#             # Compare modification time, fallback to hash check if mismatched
#             if current_stats.st_mtime != cached_stats.st_mtime:
#                 current_hash = _get_sha256_hash(path.resolve())
#                 if (self._local_file_hash_cache is None) or (
#                     self._local_file_hash_cache.get(path) != current_hash
#                 ):
#                     return False
#
#         return True
#
#     @property
#     def local_hash(self) -> Optional[dict]:
#         """Compute or retrieve the cached SHA-256 hash of the local dataset.
#
#         This property calculates the SHA-256 hash for the dataset located at `working_path`.
#         If the hash has been previously computed and cached by InputDataset.get(),
#         it will return the cached value instead of recomputing it.
#
#         If `working_path` is a list of paths, the hash is computed for each file
#         individually. The hashes are stored as a dictionary mapping paths to their
#         respective hash values.
#
#         Returns
#         -------
#         local_hash (dict or None)
#             - A dictionary where the keys are `Path` objects representing file paths
#               and the values are their respective SHA-256 hashes.
#             - `None` if `working_path` is not set or no files exist locally.
#         """
#
#         if self._local_file_hash_cache is not None:
#             return self._local_file_hash_cache
#
#         if (not self.exists_locally) or (self.working_path is None):
#             local_hash = None
#         elif isinstance(self.working_path, list):
#             local_hash = {
#                 path: _get_sha256_hash(path.resolve()) for path in self.working_path
#             }
#         elif isinstance(self.working_path, Path):
#             local_hash = {self.working_path: _get_sha256_hash(self.working_path)}
#
#         self._local_file_hash_cache = local_hash
#         return local_hash
#
#     def __str__(self) -> str:
#         name = self.__class__.__name__
#         base_str = f"{name}"
#         base_str = "-" * len(name) + "\n" + base_str
#         base_str += "\n" + "-" * len(name)
#
#         base_str += f"\nSource location: {self.location}"
#         if self.file_hash is not None:
#             base_str += f"\nSource file hash: {self.file_hash}"
#         if self.start_date is not None:
#             base_str += f"\nstart_date: {self.start_date}"
#         if self.end_date is not None:
#             base_str += f"\nend_date: {self.end_date}"
#         base_str += f"\nWorking path: {self.working_path}"
#         if self.exists_locally:
#             base_str += " (exists)"
#         else:
#             base_str += " ( does not yet exist. Call InputDataset.get() )"
#
#         if self.local_hash is not None:
#             base_str += f"\nLocal hash: {self.local_hash}"
#         return base_str
#
#     def __repr__(self) -> str:
#         # Constructor-style section:
#         repr_str = f"{self.__class__.__name__}("
#         repr_str += f"\nlocation = {self.location!r},"
#         repr_str += f"\nfile_hash = {self.file_hash!r},"
#         if self.start_date is not None:
#             repr_str += f"\nstart_date = {self.start_date!r},"
#         if self.end_date is not None:
#             repr_str += f"\nend_date = {self.end_date!r}"
#         repr_str += "\n)"
#         info_str = ""
#         if self.working_path is not None:
#             info_str += f"working_path = {self.working_path}"
#             if not self.exists_locally:
#                 info_str += " (does not exist)"
#         if self.local_hash is not None:
#             info_str += f", local_hash = {self.local_hash}"
#         if len(info_str) > 0:
#             repr_str += f"\nState: <{info_str}>"
#         # Additional info
#         return repr_str
#
#     def to_dict(self) -> dict:
#         """Represent this InputDataset object as a dictionary of kwargs.
#
#         Returns:
#         --------
#         input_dataset_dict (dict):
#            A dictionary of kwargs that can be used to initialize the
#            InputDataset object.
#         """
#         input_dataset_dict = {}
#         input_dataset_dict["location"] = self.location
#         if self.file_hash is not None:
#             input_dataset_dict["file_hash"] = self.file_hash
#         if self.start_date is not None:
#             input_dataset_dict["start_date"] = self.start_date.__str__()
#         if self.end_date is not None:
#             input_dataset_dict["end_date"] = self.end_date.__str__()
#
#         return input_dataset_dict
#
#     def get(self, local_dir: str | Path) -> None:
#         """Make the file containing this input dataset available in `local_dir`
#
#         If InputDataset.source.location_type is...
#            - ...a local path: create a symbolic link to the file in `local_dir`.
#            - ...a URL: fetch the file to `local_dir` using Pooch
#
#         This method updates the `InputDataset.working_path` attribute with the new location,
#         and caches file metadata and checksum values.
#
#         Parameters:
#         -----------
#         local_dir: str
#             The local directory in which this input dataset will be saved.
#         """
#         Path(local_dir).expanduser().mkdir(parents=True, exist_ok=True)
#         target_path = Path(local_dir).expanduser().resolve() / self.basename
#
#         if (self.exists_locally) and (self.working_path == target_path):
#             print(f"Input dataset already exists at {self.working_path}, skipping.")
#             return
#
#         if self.location_type == "path":
#             source_location = Path(self.location).expanduser().resolve()
#             computed_source_hash = _get_sha256_hash(source_location)
#             if (self.file_hash is not None) and (
#                 self.file_hash != computed_source_hash
#             ):
#                 raise ValueError(
#                     f"The provided file hash ({self.file_hash}) does not match "
#                     f"that of the file at {source_location} ({computed_source_hash}). "
#                     "Note that as this input dataset exists on the local filesystem, "
#                     "C-Star does not require a file hash to use it. Please either "
#                     "update the file_hash entry or remove it."
#                 )
#
#             target_path.symlink_to(source_location)
#
#         elif self.location_type == "url":
#             if self.file_hash is not None:
#                 downloader = pooch.HTTPDownloader(timeout=120)
#                 to_fetch = pooch.create(
#                     path=local_dir,
#                     # urllib equivalent to Path.parent:
#                     base_url=urljoin(self.location, "."),
#                     registry={self.basename: self.file_hash},
#                 )
#                 to_fetch.fetch(self.basename, downloader=downloader)
#                 computed_source_hash = self.file_hash
#             else:
#                 raise ValueError(
#                     "InputDataset.source.source_type is 'url' "
#                     + "but no InputDataset.source.file_hash is not defined. "
#                     + "Cannot proceed."
#                 )
#         self.working_path = target_path
#         self._local_file_hash_cache = {target_path: computed_source_hash}  # 27
#         self._local_file_stat_cache = {target_path: target_path.stat()}


if __name__ == "__main__":
    example_yaml = "examples/roms_marbl_blueprint.yaml"
    bp = RomsMarblBlueprint.model_validate_yaml(example_yaml)
