# OceanDB Documentation

## Building
To build this documentation, first install OceanDB from the root-level of this repository:
```sh
pip install .
```

Subsequently, install the docs-specific dependencies:
```sh
cd docs/
pip install -r requirements.txt
```

Lastly, build the docs
```sh
make html
```

The documentation should appear under `docs/build/`.
