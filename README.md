## Installing

Clone the repository to an experimental account, preferably on fishtank.
Install on your user account by running:

```
python3 setup.py install --user

```

This should install the necessary packages.

## Running

Now to convert files, open a new python file and import the module:

```
import evth5
```

The simplest use case is to convert a single file (all segments will be converted into a single file):

```
evth5.convert_run(experiment number, run number)
```
,where experiment number should be your experimental account number, e.g: 20019, 20008, etc.

You most likely want to save these files in a place with adequate storage, for example: /mnt/analysis/h5_files/,
to do this pass the path string to the h5_path argument:

```
evth5.convert_run(20019, 219, h5_path='/mnt/analysis/e20019/h5_files/')
```
note the trailing backslash.

## Data Format

h5 file is structured using [PyTables](https://www.pytables.org/). Event information is stored under

```
./raw_data/basic_info
```
waveforms (traces) are stored in 

```
./raw_data/trace_array
```

-Caleb Marshall, Ohio University 
