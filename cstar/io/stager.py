import functools
from abc import ABC
from typing import TYPE_CHECKING, ClassVar

from cstar.base.exceptions import CstarError
from cstar.io.retriever import get_retriever
from cstar.io.source_data import SupportedSources

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io import (
        LocalBinaryFileSource,
        RemoteRepositorySource,
        Retriever,
        SourceData,
        StagedFile,
        StagedRepository,
    )

from typing import Generic, TypeVar

S = TypeVar("S", bound=SourceData | RemoteRepositorySource)
StD = TypeVar("StD", bound=StagedFile | StagedRepository)


_registry: dict[SupportedSources, type["Stager"]] = {}


def register_stager(
    wrapped_cls: type["Stager"],
) -> type["Stager"]:
    """Register the decorated type as an available _SystemContext."""
    _registry[wrapped_cls._characteristics] = wrapped_cls

    @functools.wraps(wrapped_cls)
    def _inner() -> type[Stager]:
        """Return the original type after it is registered.

        Returns
        -------
        type[_SystemContext]
            The decorated type.
        """
        return wrapped_cls

    return _inner()


def get_stager(supported_source: SupportedSources) -> "Stager":
    """Retrieve a system context from the context registry.

    Returns
    -------
    _SystemContext
        The context matching the supplied name.

    Raises
    ------
    CStarError
        If the supplied name has not been registered.
    """
    if stager := _registry.get(supported_source):
        return stager()

    raise CstarError(f"No stager for {supported_source}")


class Stager(ABC, Generic[StD]):
    _characteristics: ClassVar[SupportedSources]

    def stage(self, target_dir: Path, source: SourceData) -> StD:
        """Stage this data using an appropriate strategy."""
        retrieved_path = self.retriever.save(source=source, target_dir=target_dir)
        return StagedFile(
            source=source,
            path=retrieved_path,
            sha256=getattr(source, "file_hash"),
            stat=None,
        )

    @property
    def retriever(self) -> Retriever[S]:
        return get_retriever(self._characteristics)


@register_stager
class RemoteBinaryFileStager(Stager[StagedFile]):
    _characteristics = SupportedSources.REMOTE_BINARY_FILE


@register_stager
class RemoteTextFileStager(Stager[StagedFile]):
    _characteristics = SupportedSources.REMOTE_TEXT_FILE


@register_stager
class LocalBinaryFileStager(Stager[StagedFile]):
    _characteristics = SupportedSources.LOCAL_BINARY_FILE

    # Used for e.g. a local netCDF InputDataset
    def stage(self, target_dir: Path, source: "LocalBinaryFileSource") -> "StagedFile":
        """Create a local symlink to a binary file on the current filesystem."""
        target_path = target_dir / source.filename
        target_path.symlink_to(source.location)

        return StagedFile(source=source, path=target_dir)


@register_stager
class LocalTextFileStager(Stager[StagedFile]):
    _characteristics = SupportedSources.LOCAL_TEXT_FILE


@register_stager
class RemoteRepositoryStager(Stager[StagedRepository]):
    _characteristics = SupportedSources.REMOTE_REPOSITORY

    # Used for e.g. an ExternalCodeBase
    def stage(
        self,
        target_dir: Path,
        source: "RemoteRepositorySource",
    ) -> "StagedRepository":
        """Clone and checkout a git repository at a given target."""
        retrieved_path = self.retriever.save(source=source, target_dir=target_dir)

        return StagedRepository(source=source, path=retrieved_path)
