# Continuously extending Zarr datasets

The Pangeo Project has been exploring the analysis of climate data in the cloud.
Our preferred format for storing data in the cloud is
[Zarr](https://zarr.readthedocs.io), due to its favorable interaction with
object storage. Our first Zarr cloud datasets were static, but many real
operational datasets need to be continuously updated, for example, extended in
time. In this post, we will show how we can play with Zarr to append to an
existing archive as new data becomes available.

## The problem with live data

Earth observation data which originates from e.g. satellite-based remote sensing
is produced continuously, usually with a latency that depends on the amount of
processing that is required to generate something useful for the end user. When
storing this kind of data, we obviously don't want to create a new archive from
scratch each time new data is produced, but instead append the new data to the
same archive. If this is big data, we might not even want to stage the whole
dataset on our local hard drive before uploading it to the cloud, but rather
directly stream it there. The nice thing about Zarr is that the simplicity of
its store file structure allows us to hack around and address this kind of
issue. Recent improvements to Xarray will also ease this process.

## Download the data

Let's take [TRMM 3B42RT](ftp://trmmopen.gsfc.nasa.gov/pub/merged/3B42RT) as an
example dataset (near real time, satellite-based precipitation estimates from
NASA). It is a precipitation array ranging from latitudes 60°N-S with resolution
0.25°, 3-hour, from March 2000 to present. It's a good example of a rather
obscure binary format, hidden behind a raw FTP server.

Files are organized on the server in a particular way that is specific to this
dataset, so we must have some prior knowledge of the directory structure in
order to fetch them. The following function uses the aria2 utility to download
files in parallel.

```python
from datetime import datetime, timedelta
import subprocess
import shutil

def download_files(datetime_0, datetime_nb):
    '''
    Download files from FTP server.

    Arguments:
        - datetime_0: date from which to download.
        - datetime_nb: number of dates (~files) to download.

    Returns:
        - filenames: list of file names to be downloaded.
        - datetimes: list of dates corresponding to the downloaded files.
    '''
    datetimes = [datetime_0 + timedelta(hours=3*i) for i in range(datetime_nb)]
    urls, filenames = [], []
    for dt in datetimes:
        year = dt.year
        month = str(dt.month).zfill(2)
        day = str(dt.day).zfill(2)
        hour = str(dt.hour).zfill(2)
        # file name changed suddenly one day
        if dt < datetime(2012, 11, 7, 6):
            filename = f'3B42RT.{year}{month}{day}{hour}.7R2.bin.gz'
        else:
            filename = f'3B42RT.{year}{month}{day}{hour}.7.bin.gz'
        urls.append(f'ftp://trmmopen.gsfc.nasa.gov/pub/merged/3B42RT/{year}/'
                    f'{month}/{filename}')
        filenames.append(filename)
    with open('trmm_file_list.txt', 'w') as f:
        f.write('\n'.join(urls))
    shutil.rmtree('trmm_data', ignore_errors=True)
    subprocess.check_call(f'aria2c -x 4 -i trmm_file_list.txt -d trmm_data '
                          '--continue=true'.split(),
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return filenames, datetimes
```

## Create an Xarray Dataset

In order to create an Xarray Dataset from the downloaded files, we must know how
to decode the content of the files (the binary layout of the data, its shape,
type, etc.). The following function does just that:

```python
import gzip
import xarray as xr
import numpy as np

def create_dataset(filenames, datetimes):
    '''Create a Dataset from binary files.

    Arguments:
        - filenames: list of file names to be concatenated along the time
          dimension.
        - datetimes: list of dates corresponding to the time coordinate.

    Returns:
        - ds: the Dataset created from the files and dates.
    '''
    ds_list = []
    for fname in filenames:
        f = gzip.open(f'trmm_data/{fname}', 'rb')
        data = f.read()
        f.close()
        header_size_id = 'header_byte_length='
        data_head = data[:1000]
        header_size_pos = data_head.decode('utf-8').find(header_size_id) + \
                          len(header_size_id)
        header_size = int(data[header_size_pos:data_head.decode('utf-8')
                                               .find(' ', header_size_pos)])
        data = data[header_size:header_size + 480*1440*2]
        data = np.frombuffer(data, dtype=np.dtype('>i2'), count=480*1440)
        data_array = data.reshape((480, 1440)).astype(np.float32) / 100 # mm/h
        np.clip(data_array, 0, np.inf, out=data_array)
        lat = np.arange(60-0.25/2, -60, -0.25)
        lon = np.arange(0+0.25/2, 360, 0.25)
        ds = xr.Dataset({'precipitation': (['lat', 'lon'], data_array)},
                        coords={'lat':lat, 'lon':lon})
        ds_list.append(ds)
    ds = xr.concat(ds_list, 'time')
    ds = ds.assign_coords(time=datetimes)
    return ds
```

Now we can have a nice representation of (a part of) our dataset:

```python
dt = datetime(2000, 3, 1, 12)
filenames, datetimes = download_files(dt, 40)
ds = create_dataset(filenames, datetimes)
print(ds)
```

```
<xarray.Dataset>
Dimensions:        (lat: 480, lon: 1440, time: 40)
Coordinates:
  * lat            (lat) float64 59.88 59.62 59.38 ... -59.38 -59.62 -59.88
  * lon            (lon) float64 0.125 0.375 0.625 0.875 ... 359.4 359.6 359.9
  * time           (time) datetime64[ns] 2000-03-01T12:00:00 ... 2000-03-06T09:00:00
Data variables:
    precipitation  (time, lat, lon) float32 0.0 0.0 0.0 0.0 ... 0.0 0.0 0.0 0.0
```

And plot e.g. the accumulated precipitation:

```python
import matplotlib.pyplot as plt

ds.precipitation.sum(['time']).plot(vmax=50)
plt.title('Accumulated precipitation between\n'
          '2000-03-01T12 and 2000-03-06T09 (mm)')
plt.show()
```

![alt text](https://github.com/davidbrochart/pangeo_upload/blob/master/blog_figure.png "Accumulated precipitation")

## Store the Dataset to local Zarr

This is where things start to get a bit tricky. Because the Zarr archive will be
uploaded to the cloud, it must already be chunked reasonably. There is a ~100 ms
overhead associated with every read from cloud storage. To amortize this
overhead, chunks must be bigger than 10 MiB. If we want to have several chunks
fit comfortably in memory so that they can be processed in parallel, they must
not be too big either. With today's machines, 100 MiB chunks are advised. This
means that for our dataset, we can concatenate 100 / (480 * 1440 * 4 / 1024 /
1024) ~ 40 dates into one chunk. The Zarr will be created with that chunk size.

Also, Xarray will choose some encodings for each variable when creating the Zarr
archive. The most special one is for the time variable, which will look
something like that (content of the .zattrs file):

```
{
    "_ARRAY_DIMENSIONS": [
        "time"
    ],
    "calendar": "proleptic_gregorian",
    "units": "hours since 2000-03-01 12:00:00"
}
```

It means that the time coordinate will actually be encoded as an integer
representing the number of "hours since 2000-03-01 12:00:00". When we create new
Zarr archives for new datasets, we must keep the original encodings. The
`create_zarr` function takes care of all that:

```python
def get_encoding(name):
    '''Get encodings from a Zarr archive.

    Arguments:
        - name: the name of the archive.

    Returns:
        - encoding: the encodings of the variables.
    '''
    ds = xr.open_zarr(name)
    encoding = {name: ds[name].encoding for name in list(ds.variables)}
    return encoding

def create_zarr(ds, name, encoding=None):
    '''Create a Zarr archive from an Xarray Dataset.

    Arguments:
        - ds: the Dataset to store.
        - name: the name of the Zarr archive.
        - encoding: the encoding to use for each variable.

    Returns:
        - encoding: the encoding used for each variable.
    '''
    shutil.rmtree(name, ignore_errors=True)
    ds = ds.chunk({name: ds[name].shape for name in list(ds.dims)})
    ds.to_zarr(name, encoding=encoding)
    if encoding is None:
        encoding = get_encoding(name)
    return encoding
```

## Upload the Zarr to the cloud

The first time the Zarr is created, it contains the very beginning of our
dataset, so it must be uploaded as is to the cloud. But as we download more
data, we only want to upload the new data. That's where the clear and simple
implementation of data and metadata as separate files in Zarr comes handy: as
long as the data is not accessed, we can delete the data files without
corrupting the archive. We can then append to the "empty" Zarr (but still valid
and appearing to contain the previous dataset), and upload only the necessary
files to the cloud.

One thing to keep in mind is that some coordinates (here lat and lon) won't be
affected by the append operation. Only the time coordinate and the DataArray
which depends on the time dimension (here precipitation) need to be extended.
Also, we can see that there will be a problem with the time coordinate: its
chunks will have a size of 40. That was the intention for the precipitation
variable, but because the time variable is a 1-D array, it will be much too
small. So we empty the time variable of its data for now, and it will be
uploaded later with the right chunks.

```python
import os
import zarr

def empty_zarr(name, variable=None):
    '''Empty the Zarr archive of its data (but not its metadata).

    Arguments:
        - name: the name of the archive.
        - variable: the name of the variable to empty (if None, empty all
          variables)
    '''
    for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
        if variable is not None and dname == variable:
            for fname in [f for f in os.listdir(f'{name}/{dname}')
                          if not f.startswith('.')]:
                os.remove(f'{name}/{dname}/{fname}')

def append_zarr(src_name, dst_name):
    '''Append a Zarr archive to another one.

    Arguments:
        - src_name: the name of the archive to append.
        - dst_name: the name of the archive to be appended to.
    '''
    zarr_src = zarr.open(src_name, mode='r')
    zarr_dst = zarr.open(dst_name, mode='a')
    for key in [k for k in zarr_src.array_keys() if k not in ['lat', 'lon']]:
        zarr_dst[key].append(zarr_src[key])
    empty_zarr('trmm_3b42rt', 'time')
```

## Repeat

Now that we have all the pieces, it is just a matter of putting them together in
a loop. We take care of the time coordinate by uploading in one chunk at the
end.

The following code allows to resume an upload, so that you can wait for
new data to appear on the FTP server and launch the script again:

```python
import gcsfs
import pandas as pd

dt0 = dt = datetime(2000, 3, 1, 12) # from this date (included)
dt1 = datetime(2000, 3, 11, 12)     # to that date (excluded)
#dt0 = dt = datetime(2000, 3, 11, 12) # from this date (included)
#dt1 = datetime(2000, 3, 21, 12)      # to that date (excluded)
resume = False  # if True, resume a previous upload
                # and dt0 and dt1 must be later than the previous date range
fake_gcs = True # if True, won't upload to Google Cloud Storage
                # but fake it in local trmm_bucket directory
if fake_gcs:
    store = 'trmm_bucket'
else:
    store = gcsfs.GCSMap('pangeo-data/trmm_3b42rt')
if resume:
    time_prev = xr.open_zarr(store).time.values
time_nb = 40
while dt < dt1:
    print(f'Downloading {time_nb} files from {dt}...')
    filenames, datetimes = download_files(dt, time_nb)
    ds = create_dataset(filenames, datetimes)
    if not resume and dt == dt0:
        encoding = create_zarr(ds, 'trmm_3b42rt')
        empty_zarr('trmm_3b42rt', 'time')
    else:
        if resume:
            encoding = get_encoding('trmm_3b42rt_new')
        create_zarr(ds, 'trmm_3b42rt_new', encoding)
        empty_zarr('trmm_3b42rt')
        append_zarr('trmm_3b42rt_new', 'trmm_3b42rt')
    print('Uploading...')
    if fake_gcs:
        subprocess.check_call('mkdir -p trmm_bucket; cp -r trmm_3b42rt/* '
                              'trmm_bucket/; cp -r trmm_3b42rt/.[^.]* '
                              'trmm_bucket/', shell=True)
    else:
        subprocess.check_call('gsutil -m cp -r trmm_3b42rt/ gs://pangeo-data/'
                              .split())
    dt += timedelta(hours=3*time_nb)

time_new = pd.date_range(dt0, dt1-timedelta(hours=3), freq='3H')
if resume:
    time_var = np.hstack((time_prev, time_new))
else:
    time_var = time_new
time_ds = xr.DataArray(np.zeros(len(time_var)), coords=[time_var], dims=['time']).to_dataset(name='trmm_time')
shutil.rmtree('trmm_time', ignore_errors=True)
time_ds.to_zarr('trmm_time')
if fake_gcs:
    subprocess.check_call('rm -rf trmm_bucket/time'.split())
    subprocess.check_call('cp -r trmm_time/time trmm_bucket/'.split())
else:
    subprocess.check_call('gsutil -m rm -rf '
                          'gs://pangeo-data/trmm_3b42rt/time'.split())
    subprocess.check_call('gsutil -m cp -r trmm_time/time/ '
                          'gs://pangeo-data/trmm_3b42rt/'.split())
```

## Conclusion

This post showed how to stream data directly from a provider to a cloud storage
bucket. It actually serves two purposes:

- for data that is produced continuously, we hacked around the Zarr data store
  format to efficiently append to an existing dataset.
- for data that is bigger than your hard drive, we only stage a part of the
  dataset locally and have the cloud store the totality.

An in-progress [pull request](https://github.com/pydata/xarray/pull/2706) will
give Xarray the ability to directly append to Zarr stores. Once that feature is
ready, this process may become simpler.
