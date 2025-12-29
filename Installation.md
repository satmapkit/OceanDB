## Installation



### 1. Create the directory structure and a new virtual environment

OceanDB should be installed in a parallel directory to other projects. For example, if installing [MapInterp], create a common root directory, e.g.,
```
satmapkit-dev/
├─ OceanDB/
├─ MapInterp/

```

From the command-line create the directory,
```bash
mkdir satmapkit-dev
```
and then create a new python virtual environment within that directory
  ```bash
cd satmapkit-dev
python3 -m venv .venv
source .venv/bin/activate
  ```

### 2.  Clone  and install OceanDB
  
  Clone the repo,
  ```bash
git clone https://github.com/Nazanne/OceanDB.git
```
and install it
  ```bash
cd OceanDB
pip install --upgrade pip
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

### 4. Initial the database

```bash
oceandb init
```


### 5. Install MapInterp

Go back to the root directory, check out MapInterp, and install it

```
cd ..
git clone https://github.com/satmapkit/MapInterp.git
cd MapInterp
pip install .
```

