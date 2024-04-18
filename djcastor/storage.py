from os import path as os_path
from typing import Self

from django.core.files import File
from django.core.exceptions import SuspiciousOperation
from django.core.files.storage import FileSystemStorage
from django.utils._os import safe_join
from django.utils.encoding import smart_str

from djcastor.utils import hash_filename, hash_chunks, rm_file_and_empty_parents, shard


class CAStorage(FileSystemStorage):
    """
    A content-addressable storage backend for Django.

    Basic Usage
    -----------

        from django.db import models
        from djcastor import CAStorage

        class MyModel(models.Model):
            ...
            uploaded_file = models.FileField(storage=CAStorage())

    Extended Usage
    --------------

    There are several options you can pass to the `CAStorage` constructor. The
    first two are inherited from `django.core.files.storage.FileSystemStorage`:

    *   `location`: The absolute path to the directory that will hold uploaded
        files. If omitted, this will be set to the value of the `MEDIA_ROOT`
        setting.

    *   `base_url`: The URL that serves the files stored at this location. If
        omitted, this will be set to the value of the `MEDIA_URL` setting.

    `CAStorage` also adds two custom options:

    *   `keep_extension` (default `True`): Preserve the extension on uploaded
        files. This allows the webserver to guess their `Content-Type`.

    *   `sharding` (default `(2, 2)`): The width and depth to use when sharding
        digests, expressed as a two-tuple. `django-castor` shards files in the
        uploads directory based on their digests; this prevents filesystem
        issues when too many files are in a single directory. Sharding is based
        on two parameters: *width* and *depth*. The following examples show how
        these affect the sharding:

            >>> digest = "1f09d30c707d53f3d16c530dd73d70a6ce7596a9"

            >>> print(shard(digest, width=2, depth=2))
            1f/09/1f09d30c707d53f3d16c530dd73d70a6ce7596a9

            >>> print(shard(digest, width=2, depth=3))
            1f/09/d3/1f09d30c707d53f3d16c530dd73d70a6ce7596a9

            >>> print(shard(digest, width=3, depth=2))
            1f0/9d3/1f09d30c707d53f3d16c530dd73d70a6ce7596a9
    """

    def __init__(
        self: Self,
        location: str | None = None,
        base_url: str | None = None,
        keep_extension: bool = True,
        sharding: tuple[int, ...] = (2, 2)
    ) -> None:
        # Avoid a confusing issue when you don't have a trailing slash: URLs
        # are generated which point to the parent. This is due to the behavior
        # of `urlparse.urljoin()`.
        if base_url is not None and not base_url.endswith("/"):
            base_url += "/"

        super(CAStorage, self).__init__(location=location, base_url=base_url)

        self.shard_width, self.shard_depth = sharding
        self.keep_extension = keep_extension
        
    def get_available_name(self: Self, name: str, max_length: int | None = None) -> str:
        """Return the name as-is; in CAS, given names are ignored anyway."""
        return name

    def digest(self: Self, content: File) -> str:
        if hasattr(content, "temporary_file_path"):
            return hash_filename(content.temporary_file_path())  # type: ignore
        digest = hash_chunks(content.chunks())
        content.seek(0)
        return digest

    def shard(self, hexdigest):
        return list(shard(hexdigest, self.shard_width, self.shard_depth,
                                rest_only=False))

    def path(self: Self, name: str) -> str:
        shards = self.shard(name)

        try:
            path = safe_join(self.location, *shards)
        except ValueError:
            raise SuspiciousOperation(
                f"Attempted access to '{'/'.join(shards)}' denied."
            )

        return smart_str(os_path.normpath(path))

    def url(self: Self, name: str | None) -> str:
        return super(CAStorage, self).url("/".join(self.shard(name)))

    def delete(self: Self, name: str, sure: bool = False) -> None:
        if not sure:
            # Ignore automatic deletions; we don't know how many different
            # records point to one file.
            return

        path = name
        if path.sep not in path: # type: ignore
            path = self.path(name)
        rm_file_and_empty_parents(path, root=self.location)

    def _save(self: Self, name: str, content: File) -> str:
        digest = self.digest(content)
        if self.keep_extension:
            digest += os_path.splitext(name)[1]
        path = self.path(digest)
        if os_path.exists(path):
            return digest
        return super(CAStorage, self)._save(digest, content)  # type: ignore
