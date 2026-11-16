"""Local text acquisition; durable revisions are supplied by the Rust worker."""

import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

from .arrow import Delete, Metadata, Section, Upsert, document_changes
from .config import ConfigModel, NoConfig, setting
from .connector import Connector, ContentBatch, ListingBatch


class LocalFilesScope(ConfigModel):
    paths: list[str] = setting(description="Text roots", min_items=1)
    include: list[str] = setting(description="Relative patterns", default_factory=lambda: ["**/*"])


class LocalFiles(Connector[NoConfig, LocalFilesScope]):
    Settings = NoConfig
    Scope = LocalFilesScope

    def _files(self) -> Iterator[tuple[str, Path]]:
        scope = self.context.scope
        assert isinstance(scope, LocalFilesScope)
        for index, directory in enumerate(scope.paths):
            base = Path(directory)
            if not base.is_dir():
                raise ValueError("source_unavailable")

            def unavailable(error: OSError) -> None:
                raise ValueError("source_unavailable") from error

            for parent, directories, names in os.walk(base, followlinks=False, onerror=unavailable):
                directories[:] = sorted(
                    d for d in directories if not (Path(parent) / d).is_symlink()
                )
                for name in sorted(names):
                    path = Path(parent) / name
                    if path.is_symlink() or not path.is_file():
                        continue
                    relative = path.relative_to(base)
                    if not any(
                        relative.match(p) or relative.match(p.removeprefix("**/"))
                        for p in scope.include
                    ):
                        continue
                    root = str(relative) if len(scope.paths) == 1 else f"{index}/{relative}"
                    yield root, path

    def _path(self, root: str) -> Path:
        scope = self.context.scope
        assert isinstance(scope, LocalFilesScope)
        if len(scope.paths) == 1:
            return Path(scope.paths[0]) / root
        index, relative = root.split("/", 1)
        return Path(scope.paths[int(index)]) / relative

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        if self.context.permissions != "fixed" or not self.context.readers:
            raise ValueError("local_files_readers_invalid")
        changes: list[Upsert | Delete] = []
        batch_number = 0

        def batch() -> ContentBatch:
            nonlocal batch_number
            batch_number += 1
            fingerprint = hashlib.sha256()
            fingerprint.update((cursor or "").encode())
            fingerprint.update(str(batch_number).encode())
            for change in changes:
                fingerprint.update(change.root_id.encode())
                fingerprint.update((change.source_revision or "delete").encode())
            result = ContentBatch(changes=document_changes(changes), cursor=fingerprint.hexdigest())
            changes.clear()
            return result

        page: list[tuple[str, Path]] = []
        files = iter(self._files())
        while True:
            page.clear()
            for _ in range(min(256, self.context.limits.max_rows)):
                item = next(files, None)
                if item is None:
                    break
                page.append(item)
            if not page:
                break
            known = (
                self.context.lookup_revisions([root for root, _ in page])
                if self.context.lookup_revisions
                else {}
            )
            for root, path in page:
                fingerprint = hashlib.sha256()
                for token in sorted(self.context.readers):
                    encoded = token.encode("utf-8")
                    fingerprint.update(len(encoded).to_bytes(8, "big"))
                    fingerprint.update(encoded)
                data = bytearray()
                oversized = False
                with path.open("rb") as stream:
                    while piece := stream.read(65536):
                        fingerprint.update(piece)
                        if len(data) + len(piece) <= self.context.limits.document_max_bytes:
                            data.extend(piece)
                        else:
                            oversized = True
                revision = fingerprint.hexdigest()
                if known.get(root) == revision:
                    continue
                try:
                    text = "\0" if oversized else data.decode("utf-8")
                except UnicodeDecodeError:
                    text = "\0"
                changes.append(
                    Upsert(
                        root_id=root,
                        source_revision=revision,
                        metadata=Metadata(
                            source_uri=path.absolute().as_uri(), source_type="local-files"
                        ),
                        sections=[Section(text=text)],
                        readers=self.context.readers,
                    )
                )
                if len(changes) >= min(256, self.context.limits.max_rows):
                    yield batch()
        if self.context.next_revisions:
            while revisions := self.context.next_revisions():
                for root, _ in revisions:
                    if self._path(root).exists():
                        continue
                    if self.context.lookup_revisions is None:
                        raise ValueError("local_files_revision_view_unavailable")
                    if self.context.lookup_revisions([root]).get(root) is None:
                        continue
                    changes.append(Delete(root_id=root))
                    if len(changes) >= min(256, self.context.limits.max_rows):
                        yield batch()
        if changes:
            yield batch()

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        import time

        listing_id = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        roots: list[str] = []
        part = 0
        for root, _ in self._files():
            roots.append(root)
            if len(roots) >= min(256, self.context.limits.max_rows):
                yield ListingBatch(
                    listing_id=listing_id,
                    part=part,
                    last=False,
                    complete=False,
                    root_ids=roots,
                    cursor=f"{listing_id}:{part}",
                )
                part += 1
                roots = []
        yield ListingBatch(
            listing_id=listing_id,
            part=part,
            last=True,
            complete=True,
            root_ids=roots,
            cursor=f"{listing_id}:{part}",
        )
