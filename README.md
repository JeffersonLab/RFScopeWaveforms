# rfscopedb
Software for collecting, storing and accessing Scope Mode RF Waveforms


## Quick Start Guide
Create a virtual environment using pythong 3.11+, then install the package there.  Note: update the version tag to match
desired version.

```bash
mkdir my_app
cd my_app
python.exe -m venv venv
source venv/bin/activate
pip install git+https://github.com/JeffersonLab/RFScopeWaveforms@v0.1.0
```

The repo ships with a docker compose file that will launch a simple database that holds simple test data.  Here are some
example commands to query data on all Scans from the database using this library.  This assumes that you have started
the database using `docker compose up` and that you're environment has `src/` in your python search path either by
 installing the package(e.g., `pip install .` or `pip install -e .`), manually setting PYTHONPATH variable, or other
means such as starting a python interpreter in the `src/` directory.

```python
from rfscopedb.db import WaveformDB
from rfscopedb.data_model import Query

db = WaveformDB(host='localhost', user='scope_rw', password='password')

q = Query(db=db, signal_names=["GMES", "PMES"])
# queries information on the scans that meet the criteria in q.  This should be quick.
q.stage()
# queries the waveform data related to the scans found by stage().  This may take longer as each scan can have many
# waveforms, and each waveform is 8,192 samples long.
q.run()
print(q.wf_data.head())
```

## Developer Quick Start Guide
Download the repo, create a virtual environment using pythong 3.11+.  Then develop using your preferred IDE, etc.

```bash
git clone https://github.com/JeffersonLab/RFScopeWaveforms
```

### Testing
This application supports testing using pytest.  Configuration in `pyproject.toml`.

| Test Type       | Command                   |
|-----------------|---------------------------|
| Unit            | `pytest test/unit`        |
| Integration     | `pytest test/integration` |
| All             | `pytest`                  |
