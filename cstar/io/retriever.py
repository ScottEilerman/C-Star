import functools
import hashlib
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, TypeVar

import requests

from cstar.base.exceptions import CstarError
from cstar.base.gitutils import _checkout, _clone
from cstar.io import (
    LocalSourceData,
    RemoteBinaryFileSource,
    RemoteRepositorySource,
    RemoteSourceData,
    RemoteTextFileSource,
    SourceData,
)
from cstar.io.source_data import (
    LocalBinaryFileSource,
    LocalTextFileSource,
    SupportedSources,
)

SD = TypeVar("SD", bound=SourceData)
RSD = TypeVar("RSD", bound=RemoteSourceData)


_registry: dict[SupportedSources, type["Retriever"]] = {}


def register_retriever(
    wrapped_cls: type["Retriever"],
) -> type["Retriever"]:
    """Register the decorated type as an available _SystemContext."""
    _registry[wrapped_cls._characteristics] = wrapped_cls

    @functools.wraps(wrapped_cls)
    def _inner() -> type[Retriever]:
        """Return the original type after it is registered.

        Returns
        -------
        type[_SystemContext]
            The decorated type.
        """
        return wrapped_cls

    return _inner()


def get_retriever(supported_source: SupportedSources) -> "Retriever":
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
    if retriever := _registry.get(supported_source):
        return retriever()

    raise CstarError(f"No retriever for {supported_source}")


class Retriever(Generic[SD], ABC):
    @abstractmethod
    def read(self, source: SD) -> bytes:
        """Retrieve data to memory, if supported"""
        pass

    def save(self, target_dir: Path, source: SD) -> Path:
        """
        Save this object to the given directory.

        This method performs common setup ( ensuring the target directory exists)
        and then calls the subclass-defined `_save()` method for the actual save.
        """
        if target_dir.exists():
            if not target_dir.is_dir():
                raise ValueError(
                    f"Cannot save to target_dir={target_dir} (not a directory)"
                )
        else:
            target_dir.mkdir(parents=True)

        savepath = self._save(target_dir=target_dir, source=source)
        return savepath

    @abstractmethod
    def _save(self, target_dir: Path, source: SD) -> Path:
        """Retrieve data to a local path"""
        pass


class RemoteFileRetriever(Retriever, Generic[RSD], ABC):
    def read(self, source: RSD) -> bytes:
        response = requests.get(source.location, allow_redirects=True)
        response.raise_for_status()
        data = response.content

        return data

    @abstractmethod
    def _save(self, target_dir: Path, source: RSD) -> Path:
        pass


@register_retriever
class RemoteBinaryFileRetriever(RemoteFileRetriever[RemoteBinaryFileSource]):
    _characteristics = SupportedSources.REMOTE_BINARY_FILE

    def _save(
        self,
        target_dir: Path,
        source: RemoteBinaryFileSource,
    ) -> Path:
        hash_obj = hashlib.sha256()

        target_path = target_dir / source.filename

        with requests.get(source.location, stream=True, allow_redirects=True) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):  # Download in 8kB chunks
                    if chunk:
                        f.write(chunk)
                        hash_obj.update(chunk)

        # Hash verification if specified in SourceData:
        if source.file_hash:
            actual_hash = hash_obj.hexdigest()
            expected_hash = source.file_hash.lower()

            if actual_hash != expected_hash:
                target_path.unlink(missing_ok=True)  # Remove downloaded file
                raise ValueError(
                    f"Hash mismatch for {source.location} (saved file):\n"
                    f"Expected: {expected_hash}\nActual:   {actual_hash}.\n"
                    f"File deleted for safety."
                )
        return target_path


@register_retriever
class RemoteTextFileRetriever(RemoteFileRetriever):
    _characteristics = SupportedSources.REMOTE_TEXT_FILE

    def _save(self, target_dir: Path, source: RemoteTextFileSource) -> Path:
        data = self.read(source=source)
        target_path = target_dir / source.filename
        with open(target_path, "wb") as f:
            f.write(data)
        return target_path


class LocalFileRetriever(Retriever[LocalSourceData]):
    def read(self, source: LocalSourceData) -> bytes:
        with open(source.location, "rb") as f:
            return f.read()

    def _save(self, target_dir: Path, source: LocalSourceData) -> Path:
        target_path = target_dir / source.filename
        shutil.copy2(src=Path(source.location).resolve(), dst=target_path)
        return target_path


@register_retriever
class LocalBinaryFileRetriever(LocalFileRetriever[LocalBinaryFileSource]):
    _characteristics = SupportedSources.LOCAL_BINARY_FILE


@register_retriever
class LocalTextFileRetriever(LocalFileRetriever[LocalTextFileSource]):
    _characteristics = SupportedSources.LOCAL_TEXT_FILE


@register_retriever
class RemoteRepositoryRetriever(Retriever[RemoteRepositorySource]):
    _characteristics = SupportedSources.REMOTE_REPOSITORY

    def read(self, source: RemoteRepositorySource) -> bytes:
        raise NotImplementedError("Cannot 'read' a remote repository to memory")

    def _save(self, target_dir: Path, source: RemoteRepositorySource) -> Path:
        _clone(
            source_repo=source.location,
            local_path=target_dir,
        )
        if source.checkout_target:
            _checkout(
                source_repo=source.location,
                local_path=target_dir,
                checkout_target=source.checkout_target,
            )
        return target_dir
