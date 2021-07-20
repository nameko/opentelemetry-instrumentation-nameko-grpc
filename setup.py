#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import find_packages, setup


BASE_DIR = os.path.dirname(__file__)
PACKAGE_INFO = {}

VERSION_FILENAME = os.path.join(BASE_DIR, "nameko_grpc_opentelemetry", "version.py")
with open(VERSION_FILENAME) as f:
    exec(f.read(), PACKAGE_INFO)

PACKAGE_FILENAME = os.path.join(BASE_DIR, "nameko_grpc_opentelemetry", "package.py")
with open(PACKAGE_FILENAME) as f:
    exec(f.read(), PACKAGE_INFO)


setup(
    name="opentelemetry-instrumentation-nameko-grpc",
    description="Extension producing opentelemetry data for nameko-grpc",
    version=PACKAGE_INFO["__version__"],
    author="Nameko Authors",
    url="https://github.com/nameko/opentelemetry-instrumentation-nameko-grpc",
    packages=find_packages(exclude=["test", "test.*"]),
    install_requires=[
        "nameko",
        "nameko-grpc>=1.2.0rc",
        "googleapis-common-protos",  # should be a nameko-grpc dep
        "opentelemetry-api",
        "opentelemetry-instrumentation",
        "opentelemetry-instrumentation-nameko>=0.3.0",
    ],
    extras_require={
        "dev": list(PACKAGE_INFO["_instruments"])
        + [
            "coverage",
            "pytest",
            "opentelemetry-sdk",
            "opentelemetry-instrumentation-requests",
            "grpcio-tools",
        ],
        "instruments": PACKAGE_INFO["_instruments"],
    },
    dependency_links=[],
    zip_safe=True,
    license="Apache License, Version 2.0",
)
