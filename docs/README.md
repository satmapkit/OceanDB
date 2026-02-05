# OceanDB Documentation

## Building
To build this documentation, first install the OceanDB dev dependencies from the root-level of this repository:
```sh
pip install .[dev]
```

Next, build the docs
```sh
cd docs/
make html
```

The documentation should appear under `docs/build/`.
