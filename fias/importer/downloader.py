import contextlib
import tempfile
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Callable, List, Tuple
from urllib.error import ContentTooShortError
from urllib.request import Request, urlopen


class Downloader:
    class RetriesExceededError(Exception):
        pass

    _url_temp_files: List[Path]

    def __init__(self) -> None:
        super().__init__()
        self._url_temp_files = []

    def _open_file(self, file_name: Path | None = None, append_file: bool = False) -> Tuple[IO[bytes], Path, bool]:
        if file_name is None and append_file:
            raise ValueError("append_file requires file_name")
        is_new = True
        if file_name:
            if append_file:
                is_new = False
                mode = "ab"
            else:
                mode = "wb"
            tfp = file_name.open(mode)
        else:
            tfp = tempfile.NamedTemporaryFile(delete=False)
            file_name = Path(tfp.name)
            self._url_temp_files.append(file_name)
        return tfp, file_name, is_new

    def _get(
        self,
        url: str,
        tfp: IO[bytes],
        file_name: Path,
        is_new: bool,
        reporthook: Callable[[int, int, int], None] | None = None,
    ) -> Tuple[Path, HTTPMessage]:
        request = Request(url)
        read: int = 0
        if not is_new:
            read = file_name.stat().st_size
            request.add_header("Range", f"bytes={read}-")

        with contextlib.closing(urlopen(request)) as fp:
            headers: HTTPMessage = fp.info()
            result = file_name, headers

            with tfp:
                bs = 1024 * 8
                block_num = 0
                if not is_new and "content-range" in headers:
                    size = int(headers["content-range"].split("/")[-1])
                    block_num = read // bs
                elif "content-length" in headers:
                    size = int(headers["Content-Length"])
                    if size <= read:
                        return result

                if reporthook:
                    reporthook(block_num, bs, size)

                while True:
                    block = fp.read(bs)
                    if not block:
                        break
                    read += len(block)
                    tfp.write(block)
                    block_num += 1
                    if reporthook:
                        reporthook(block_num, bs, size)
        tfp.close()
        if size >= 0 and read < size:
            raise ContentTooShortError(
                f"retrieval incomplete: got only {read} out of {size} bytes", (str(result[0]), result[1])
            )

        return result

    def download(
        self,
        url: str,
        file_name: Path | None = None,
        append_file: bool = False,
        reporthook: Callable[[int, int, int], None] | None = None,
        retries: int = 5,
    ) -> Tuple[Path, HTTPMessage]:
        tfp, file_name, is_new = self._open_file(file_name, append_file)
        while retries > 0:
            try:
                return self._get(url, tfp, file_name, is_new, reporthook)
            except ContentTooShortError:
                retries -= 1
        raise self.RetriesExceededError()

    def urlcleanup(self) -> None:
        for temp_file in self._url_temp_files:
            try:
                temp_file.unlink(True)
            except OSError:
                pass

        del self._url_temp_files[:]
