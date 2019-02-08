#!/usr/bin/env python3
"""Run benchmark tests."""

import concurrent.futures
import enum
import os
import pathlib
import subprocess
import time
import uuid
from typing import Any, Callable, List, Tuple, Union

import gsutilwrap
import prettytable
import temppathlib

import gswrap

# pylint: disable=missing-docstring


class LibraryChecked(enum.Enum):
    """Store names of all libraries which are benchmarked against gswrap."""
    gsutilwrap = 1


def timer(func: Callable[..., Any], *args: Any, **kwargs: Any) -> float:
    """Return time a method needs."""
    start = time.time()
    func(*args, **kwargs)
    end = time.time()
    return end - start


def print_benchmark(
        benchmark: str, time_gswrap: float,
        time_other_libraries: List[Tuple[LibraryChecked, float]]) -> None:
    """Print benchmark table with all libraries that were tested."""

    time_table = prettytable.PrettyTable(
        hrules=prettytable.ALL, header_style="upper")
    time_table.add_column(fieldname="Tested", column=[], align='l')
    time_table.add_column(fieldname="Time", column=[], align='l')
    time_table.add_column(fieldname="SpeedUp", column=[], align='l')
    time_table.add_row(["gswrap", "{} s".format(round(time_gswrap, 2)), r"\-"])
    for library, time_other in time_other_libraries:
        time_table.add_row([
            "{}".format(library.name), "{} s".format(round(time_other, 2)),
            "{} x".format(round(time_other / time_gswrap, 2))
        ])

    print("{}:\n\n{}\n".format(benchmark, time_table))


def _setup(path: pathlib.Path, url: str) -> None:
    """
    Upload files from given path to self.url_prefix on google cloud storage.
    """
    subprocess.check_call([
        "gsutil", "-m", "-q", "cp", "-r", "{}".format(path.as_posix()),
        "{}".format(url)
    ])


def _tear_down(url: str) -> None:
    """Clean up defined url on google cloud storage."""

    subprocess.check_call(["gsutil", "-m", "-q", "rm", "-r", "{}".format(url)])


def _gswrap_cp(client: gswrap.Client, src: Union[str, pathlib.Path],
               dst: Union[str, pathlib.Path]) -> None:
    """
    Wrap gswrap copy default benchmark setting.
    """
    client.cp(src=src, dst=dst, recursive=True, multithreaded=True)


def _gsutilwrap_cp(src: Union[str, pathlib.Path],
                   dst: Union[str, pathlib.Path]) -> None:
    """
    Wrap gsutilwrap copy default benchmark setting.
    """
    gsutilwrap.copy(
        pattern=src, target=dst, quiet=True, multithreaded=True, recursive=True)


def _upload_many_to_many_local_ls(src: str, dst: str) \
        -> List[Tuple[str, str]]:
    srcs_dsts = []

    for root, dirs, files in os.walk(src):  # pylint: disable=unused-variable
        for file in files:
            src_pth = root + '/' + file
            dst_url = src_pth.replace(src, dst)
            srcs_dsts.append((src_pth, dst_url))

    return srcs_dsts


def _gswrap_list_for_cp_many_to_many(client: gswrap.Client, src: str,
                                     dst: str) -> List[Tuple[str, str]]:
    lst = client.ls(url=src, recursive=True)
    srcs_dsts = []
    for file in lst:
        srcs_dsts.append((file, file.replace(src, dst)))

    return srcs_dsts


def _gsutilwrap_list_for_cp_many_to_many(src: str,
                                         dst: str) -> List[Tuple[str, str]]:
    lst = gsutilwrap.ls(src + "**")
    srcs_dsts = []
    for file in lst:
        srcs_dsts.append((file, file.replace(src, dst)))

    return srcs_dsts


def _gsutilwrap_download_many_to_many_setup(src: str, dst: str) \
        -> List[Tuple[str, str]]:
    srcs_dsts = _gsutilwrap_list_for_cp_many_to_many(src=src, dst=dst)

    # directory structure needs to first be created with gsutilwrap
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executer:
        for src_dst in srcs_dsts:
            dst_pth = pathlib.Path(src_dst[1])
            mkdir_thread = executer.submit(
                dst_pth.parent.mkdir, parents=True, exist_ok=True)
            futures.append(mkdir_thread)

    for future in futures:
        future.result()

    return srcs_dsts


def _gswrap_copy_many_to_many_files(client: gswrap.Client, srcs_dsts: List[
        Tuple[Union[pathlib.Path, str], Union[pathlib.Path, str]]]) -> None:

    client.cp_many_to_many(
        srcs_dsts=srcs_dsts, recursive=True, multithreaded=True)


def _gsutilwrap_copy_many_to_many_files(srcs_dsts: List[
        Tuple[Union[pathlib.Path, str], Union[pathlib.Path, str]]]) -> None:

    gsutilwrap.copy_many_to_many(
        patterns_targets=srcs_dsts,
        quiet=True,
        multithreaded=True,
        recursive=True)


class Benchmark:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.url_prefix = "gs://{}/{}".format(bucket, str(uuid.uuid4()))

    def run(self) -> None:
        """Run all benchmarks."""
        self.benchmark_list_many_files()
        self.benchmark_upload_many_files()
        self.benchmark_upload_many_single_files()
        self.benchmark_upload_big_files()
        self.benchmark_upload_many_to_many()
        self.benchmark_download_many_files()
        self.benchmark_download_many_to_many()
        self.benchmark_copy_many_files_on_remote()
        self.benchmark_copy_many_to_many_on_remote()
        self.benchmark_rm_many()
        self.benchmark_read_many()
        self.benchmark_write_many()
        self.benchmark_stat_many()

    def benchmark_list_many_files(self) -> None:
        for testcase in [10, 1000, 10**4]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("text")
                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)
                    client = gswrap.Client()
                    time_gswrap = timer(
                        client.ls, url=self.url_prefix, recursive=True)

                    time_gsutilwrap = timer(gsutilwrap.ls, self.url_prefix)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark list {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_upload_many_files(self) -> None:
        for testcase in [10, 1000, 10**4]:

            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("text")
                try:
                    client = gswrap.Client()
                    time_gswrap = timer(
                        _gswrap_cp,
                        src=tmp_dir.path,
                        dst=self.url_prefix,
                        client=client)
                finally:
                    _tear_down(url=self.url_prefix)

                try:
                    time_gsutilwrap = timer(
                        _gsutilwrap_cp, src=tmp_dir.path, dst=self.url_prefix)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark upload {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_upload_many_single_files(self) -> None:
        for testcase in [10, 25]:

            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("text")
                try:
                    client = gswrap.Client()
                    time_gswrap = 0.0
                    for file in tmp_dir.path.iterdir():
                        time_gswrap += timer(
                            _gswrap_cp,
                            src=file,
                            dst=self.url_prefix,
                            client=client)
                finally:
                    _tear_down(url=self.url_prefix)

                try:
                    time_gsutilwrap = 0.0
                    for file in tmp_dir.path.iterdir():
                        time_gsutilwrap += timer(
                            _gsutilwrap_cp, src=file, dst=self.url_prefix)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark upload {} single files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_upload_big_files(self) -> None:
        number_of_files = 3
        for size in [10, 1024, 1024**2, 200 * 1024**2]:  # bytes

            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(number_of_files):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("a" * size)
                try:
                    client = gswrap.Client()
                    time_gswrap = timer(
                        _gswrap_cp,
                        src=tmp_dir.path,
                        dst=self.url_prefix,
                        client=client)
                finally:
                    _tear_down(url=self.url_prefix)

                try:
                    time_gsutilwrap = timer(
                        _gsutilwrap_cp, src=tmp_dir.path, dst=self.url_prefix)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark upload 3 files with {} bytes".format(size),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_upload_many_to_many(self) -> None:
        for testcase in [10, 100, 500]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "{}/file".format(index)
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.write_text("text")
                try:
                    client = gswrap.Client()
                    srcs_dsts = _upload_many_to_many_local_ls(
                        src=tmp_dir.path.as_posix(),
                        dst=self.url_prefix + '/gswrap')
                    time_gswrap = timer(
                        _gswrap_copy_many_to_many_files,
                        srcs_dsts=srcs_dsts,
                        client=client)

                    srcs_dsts = _upload_many_to_many_local_ls(
                        src=tmp_dir.path.as_posix(),
                        dst=self.url_prefix + '/gsutilwrap')
                    time_gsutilwrap = timer(
                        _gsutilwrap_copy_many_to_many_files,
                        srcs_dsts=srcs_dsts)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark upload-many-to-many {} files".format(
                    testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_download_many_files(self) -> None:
        for testcase in [10, 1000, 10**4]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("hello")
                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    gsutil_dir = tmp_dir.path / "gsutil"
                    gsutil_dir.mkdir()
                    time_gsutilwrap = timer(
                        _gsutilwrap_cp, src=self.url_prefix, dst=gsutil_dir)

                    gswrap_dir = tmp_dir.path / "gswrap"
                    gswrap_dir.mkdir()
                    client = gswrap.Client()
                    time_gswrap = timer(
                        _gswrap_cp,
                        client=client,
                        src=self.url_prefix,
                        dst=gswrap_dir)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark download {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_download_many_to_many(self) -> None:
        for testcase in [10, 100, 500]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "{}/file".format(index)
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.write_text("text")
                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    gswrap_dir = tmp_dir.path / "gswrap"
                    gswrap_dir.mkdir()
                    client = gswrap.Client()
                    srcs_dsts = _gswrap_list_for_cp_many_to_many(
                        client=client,
                        src=self.url_prefix,
                        dst=gswrap_dir.as_posix())

                    time_gswrap = timer(
                        _gswrap_copy_many_to_many_files,
                        client=client,
                        srcs_dsts=srcs_dsts)

                    gsutil_dir = tmp_dir.path / "gsutil"
                    gsutil_dir.mkdir()

                    srcs_dsts = _gsutilwrap_download_many_to_many_setup(
                        src=self.url_prefix, dst=gsutil_dir.as_posix())

                    time_gsutilwrap = timer(
                        _gsutilwrap_copy_many_to_many_files,
                        srcs_dsts=srcs_dsts)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark download-many-to-many {} files".format(
                    testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_copy_many_files_on_remote(self) -> None:
        for testcase in [10, 100, 1000]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("hello")

                copy_url = "gs://{}/{}".format(self.bucket, str(uuid.uuid4()))
                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    time_gsutilwrap = timer(
                        _gsutilwrap_cp,
                        src=self.url_prefix,
                        dst=copy_url + "/gsutil")

                    client = gswrap.Client()
                    time_gswrap = timer(
                        _gswrap_cp,
                        client=client,
                        src=self.url_prefix,
                        dst=copy_url + "/gswrap")
                finally:
                    _tear_down(url=self.url_prefix)
                    _tear_down(url=copy_url)

            print_benchmark(
                benchmark="Benchmark copy on remote {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_copy_many_to_many_on_remote(self) -> None:
        for testcase in [10, 100, 500]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "{}/file".format(index)
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.write_text("text")

                copy_url = "gs://{}/{}".format(self.bucket, str(uuid.uuid4()))
                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    client = gswrap.Client()
                    srcs_dsts = _gswrap_list_for_cp_many_to_many(
                        client=client,
                        src=self.url_prefix,
                        dst=copy_url + "/gswrap")
                    time_gswrap = timer(
                        _gswrap_copy_many_to_many_files,
                        client=client,
                        srcs_dsts=srcs_dsts)

                    srcs_dsts = _gsutilwrap_list_for_cp_many_to_many(
                        src=self.url_prefix, dst=copy_url + "/gsutil")
                    time_gsutilwrap = timer(
                        _gsutilwrap_copy_many_to_many_files,
                        srcs_dsts=srcs_dsts)
                finally:
                    _tear_down(url=self.url_prefix)
                    _tear_down(url=copy_url)

            print_benchmark(
                benchmark="Benchmark copy-many-to-many-on-remote"
                " {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_rm_many(self) -> None:
        for testcase in [10, 100, 1000]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("hello")

                _setup(url=self.url_prefix, path=tmp_dir.path)

                time_gsutilwrap = timer(
                    gsutilwrap.remove,
                    pattern=self.url_prefix,
                    quiet=True,
                    multithreaded=True,
                    recursive=True)

                _setup(url=self.url_prefix, path=tmp_dir.path)

                client = gswrap.Client()
                time_gswrap = timer(
                    client.rm,
                    url=self.url_prefix,
                    recursive=True,
                    multithreaded=True)

            print_benchmark(
                benchmark="Benchmark remove {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_read_many(self) -> None:
        for testcase in [10, 100]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("hello")

                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    time_gsutilwrap = 0.0
                    urls = gsutilwrap.ls(self.url_prefix + "**")
                    for url in urls:
                        time_gsutilwrap += timer(gsutilwrap.read_text, url=url)

                    client = gswrap.Client()
                    time_gswrap = 0.0
                    urls = client.ls(url=self.url_prefix, recursive=True)
                    for url in urls:
                        time_gswrap += timer(client.read_text, url=url)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark read {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_write_many(self) -> None:
        for testcase in [10, 30]:
            try:
                time_gsutilwrap = 0.0
                for index in range(testcase):
                    time_gsutilwrap += timer(
                        gsutilwrap.write_text,
                        url="{}/gsutil/file{}".format(self.url_prefix, index),
                        text="hello",
                        quiet=True)

                client = gswrap.Client()
                time_gswrap = 0.0
                for index in range(testcase):
                    time_gswrap += timer(
                        client.write_text,
                        url="{}/gswrap/file{}".format(self.url_prefix, index),
                        text="hello")
            finally:
                _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark write {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)

    def benchmark_stat_many(self) -> None:
        for testcase in [10, 100]:
            with temppathlib.TemporaryDirectory() as tmp_dir:
                for index in range(testcase):
                    file = tmp_dir.path / "file{}".format(index)
                    file.write_text("hello")

                try:
                    _setup(url=self.url_prefix, path=tmp_dir.path)

                    time_gsutilwrap = 0.0
                    urls = gsutilwrap.ls(self.url_prefix + "**")
                    for url in urls:
                        time_gsutilwrap += timer(gsutilwrap.stat, url=url)

                    client = gswrap.Client()
                    time_gswrap = 0.0
                    urls = client.ls(url=self.url_prefix, recursive=True)
                    for url in urls:
                        time_gswrap += timer(client.stat, url=url)
                finally:
                    _tear_down(url=self.url_prefix)

            print_benchmark(
                benchmark="Benchmark stat {} files".format(testcase),
                time_other_libraries=[(LibraryChecked.gsutilwrap,
                                       time_gsutilwrap)],
                time_gswrap=time_gswrap)
