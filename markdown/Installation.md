## Installation



### 1. Create the directory structure and a new virtual environment

OceanDB should be installed in a parallel directory to other projects. For example, if installing [MapInterp], create a common root directory, e.g.,
```
satmapkit-dev/
├─ OceanDB/
```

  ```bash
python3 -m venv .venv
source .venv/bin/activate
  ```

### 2.  Clone  and install OceanDB
Clone the repo & create the virtual environment
  ```bash
git clone https://github.com/Nazanne/OceanDB.git
cd OceanDB
python3 -m venv .venv
source .venv/bin/activate
pip install -e . // Installs in editable mode
```

### 3a. Install the the PostgreSQL server from a macOS download
  
 Download [the installer](https://postgresapp.com), copy the `Postgres.app` to your applications folder, open up the app and then hit `initialize`.  Th PostgreSQL server is running locally on our machine.

We now need to tell OceanDB where the server is. From within the OceanDB folder, copy the .env file,
```bash
cp .env.example .env
```
and set the database permissions within the .env file,
```bash
POSTGRES_HOST=localhost
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocean
```

### 4. Initialize the database

```bash
oceandb init
```

