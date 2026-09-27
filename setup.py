from __future__ import annotations

import multiprocessing
import os
import re
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path

from cykit._build.config import config as cykit_config
from Cython.Build import cythonize
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext as _build_ext

ROOT = Path(__file__).resolve().parent
PKG_DIR = ROOT / "fsticker"
CMAKE_BUILD_DIR = ROOT / "_cmake_build"


def _read_cmake_cache_var(build_dir: Path, name: str) -> str | None:
    cache_file = build_dir / "CMakeCache.txt"
    if not cache_file.exists():
        return None
    pattern = re.compile(rf"^{re.escape(name)}:[A-Z]+=(.*)$")
    for line in cache_file.read_text().splitlines():
        m = pattern.match(line)
        if m and m.group(1):
            return m.group(1)
    return None


_OPENSSL_LIB_NAMES = frozenset({"ssl", "crypto"})
_WINDOWS_STATIC_SYSTEM_LIBS = (
    "ws2_32",
    "crypt32",
    "user32",
    "advapi32",
    "gdi32",
    "bcrypt",
)


def _unique(items: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(items))


def _read_cmake_paths_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            key, _, value = line.partition("=")
            data[key.strip()] = value.strip()
    return data


def _resolve_cmake_lib(raw: str) -> str:
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    if not parts:
        return ""
    if parts[0].lower() not in ("optimized", "debug", "general"):
        return parts[0]
    for keyword, lib in zip(parts[0::2], parts[1::2]):
        if keyword.lower() in ("optimized", "general"):
            return lib
    return ""


def _openssl_link_settings(
    paths: dict[str, str], link_static: bool
) -> tuple[list[str], list[str], list[str], list[str]]:
    include_dirs = [
        d
        for d in paths.get("OPENSSL_INCLUDE_DIR", "").split(";")
        if d and Path(d).is_dir()
    ]
    lib_files = [
        lib
        for lib in (
            _resolve_cmake_lib(paths.get(key, ""))
            for key in ("OPENSSL_SSL_LIBRARY", "OPENSSL_CRYPTO_LIBRARY")
        )
        if lib
    ]

    if sys.platform == "win32":
        objects = [lib for lib in lib_files if Path(lib).is_file()]
        libraries = list(_WINDOWS_STATIC_SYSTEM_LIBS) if link_static else ["crypt32"]
        return include_dirs, [], libraries, objects

    library_dirs = _unique(str(Path(lib).parent) for lib in lib_files)
    libraries: list[str] = []
    for lib in lib_files:
        match = re.match(r"^lib([^.]+)\.", Path(lib).name)
        libraries.append(match.group(1) if match else Path(lib).stem)
    if link_static and sys.platform.startswith("linux"):
        libraries.extend(("dl", "pthread"))
    return include_dirs, library_dirs, _unique(libraries), []


class CMakeBuildExt(_build_ext):
    def run(self):
        self._configure_cmake()
        self._resolve_rapidjson()
        self._resolve_fastfloat()
        self._resolve_openssl()
        self._resolve_postgres()
        super().run()

    def _platform_configure_args(self) -> list[str]:
        if sys.platform == "darwin":
            archs = re.findall(r"-arch\s+(\S+)", os.environ.get("ARCHFLAGS", ""))
            args = []
            if archs:
                args.append(f"-DCMAKE_OSX_ARCHITECTURES={';'.join(archs)}")
            deployment_target = os.environ.get("MACOSX_DEPLOYMENT_TARGET")
            if deployment_target:
                args.append(f"-DCMAKE_OSX_DEPLOYMENT_TARGET={deployment_target}")
            return args
        elif sys.platform == "win32" and not os.environ.get("CMAKE_GENERATOR"):
            arch = {
                "win-amd64": "x64",
                "win-arm64": "ARM64",
                "win32": "Win32",
            }.get(self.plat_name)
            if arch:
                return ["-A", arch]
        return []

    def _configure_cmake(self) -> None:
        shutil.rmtree(CMAKE_BUILD_DIR, ignore_errors=True)
        CMAKE_BUILD_DIR.mkdir(exist_ok=True)

        self.spawn(
            [
                "cmake",
                "-S",
                str(ROOT),
                "-B",
                str(CMAKE_BUILD_DIR),
                "-DCMAKE_BUILD_TYPE=Release",
                "-DCMAKE_CONFIGURATION_TYPES=Release",
                *self._platform_configure_args(),
            ]
        )

    def _resolve_rapidjson(self) -> None:
        rapidjson_include = _read_cmake_cache_var(
            CMAKE_BUILD_DIR, "RAPIDJSON_INCLUDE_DIR"
        )
        if not rapidjson_include:
            raise RuntimeError(
                "CMake configure didn't resolve RAPIDJSON_INCLUDE_DIR -- "
                "check the [RapidJSON] messages in the configure output above."
            )

        for ext in self.extensions:
            if rapidjson_include not in ext.include_dirs:
                ext.include_dirs.append(rapidjson_include)

    def _resolve_fastfloat(self) -> None:
        fastfloat_include = _read_cmake_cache_var(
            CMAKE_BUILD_DIR, "FASTFLOAT_INCLUDE_DIR"
        )
        if not fastfloat_include:
            raise RuntimeError(
                "CMake configure didn't resolve FASTFLOAT_INCLUDE_DIR -- "
                "check the [fast_float] messages in the configure output above."
            )

        for ext in self.extensions:
            if fastfloat_include not in ext.include_dirs:
                ext.include_dirs.append(fastfloat_include)

    def _resolve_openssl(self) -> None:
        paths = _read_cmake_paths_file(CMAKE_BUILD_DIR / "openssl_paths.txt")
        if not paths:
            raise RuntimeError(
                "CMake configure didn't produce openssl_paths.txt -- "
                "check the [OpenSSL] messages in the configure output above."
            )

        from_source = paths.get("OPENSSL_FROM_FETCHCONTENT") == "1"
        link_static = from_source or paths.get("OPENSSL_LINK_STATIC") == "1"
        if from_source:
            self.spawn(
                [
                    "cmake",
                    "--build",
                    str(CMAKE_BUILD_DIR),
                    "--config",
                    "Release",
                    "--target",
                    "ssl",
                    "crypto",
                    "--parallel",
                    str(os.cpu_count() or 1),
                ]
            )

        include_dirs, library_dirs, libraries, objects = _openssl_link_settings(
            paths, link_static
        )
        if not include_dirs or not (libraries or objects):
            raise RuntimeError(
                "OpenSSL include/library paths could not be resolved from "
                f"{CMAKE_BUILD_DIR / 'openssl_paths.txt'}."
            )

        for ext in self.extensions:
            ext.include_dirs = _unique([*ext.include_dirs, *include_dirs])
            ext.library_dirs = _unique([*library_dirs, *ext.library_dirs])
            ext.libraries = _unique(
                [
                    *(lib for lib in ext.libraries if lib not in _OPENSSL_LIB_NAMES),
                    *libraries,
                ]
            )
            ext.extra_objects = _unique([*ext.extra_objects, *objects])

    def _resolve_postgres(self) -> None:
        paths = _read_cmake_paths_file(CMAKE_BUILD_DIR / "openssl_paths.txt")
        include_dirs = [
            d for d in paths.get("FSTICKER_PQ_INCLUDE_DIRS", "").split(";") if d
        ]
        lib_files = [l for l in paths.get("FSTICKER_PQ_LIBRARIES", "").split(";") if l]
        if not include_dirs or not lib_files:
            # means FSTICKER_ENABLE_TIMESCALE=OFF for this build
            return
        for ext in self.extensions:
            ext.include_dirs = _unique([*ext.include_dirs, *include_dirs])
            ext.extra_objects = _unique([*ext.extra_objects, *lib_files])
            if sys.platform.startswith("linux"):
                ext.libraries = _unique([*ext.libraries, "resolv"])
            elif sys.platform == "win32":
                ext.libraries = _unique([*ext.libraries, "secur32", "shell32"])


_cykit_kwargs = cykit_config.get_extension_kwargs(ssl=False)

_include_dirs = list(_cykit_kwargs.get("include_dirs") or [])
for _extra in (str(ROOT / "fsticker" / "include"), str(PKG_DIR)):
    if _extra not in _include_dirs:
        _include_dirs.append(_extra)
_cykit_kwargs["include_dirs"] = _include_dirs


# Rapidjson not gonna fix this. Its better we silence the noise ::
if sys.platform == "win32":
    _define_macros = list(_cykit_kwargs.get("define_macros") or [])
    _silence_iterator_base = (
        "_SILENCE_CXX17_ITERATOR_BASE_CLASS_DEPRECATION_WARNING",
        None,
    )
    if _silence_iterator_base not in _define_macros:
        _define_macros.append(_silence_iterator_base)
    _cykit_kwargs["define_macros"] = _define_macros


ext = Extension(
    name="fsticker.feed_merge",
    sources=[
        "fsticker/feed_merge.pyx",
    ],
    **_cykit_kwargs,
)

_ext_modules = cythonize(
    [ext],
    language_level=3,
    compiler_directives=cykit_config.get_compiler_directives(),
    nthreads=multiprocessing.cpu_count(),
)

for _e in _ext_modules:
    _e.sources = [os.path.relpath(_s, ROOT) for _s in _e.sources]

setup(
    name="fsticker",
    ext_modules=_ext_modules,
    cmdclass={"build_ext": CMakeBuildExt},
    packages=["fsticker"],
    include_package_data=True,
    zip_safe=False,
)
