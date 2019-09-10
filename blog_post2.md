# Transposing chunks in Zarr datasets

This post is a follow-up of [Continuously extending Zarr
datasets](https://medium.com/pangeo/continuously-extending-zarr-datasets-c54fbad3967d),
which was specifically dealing with how to extend a Zarr dataset as data becomes
available. The underlying assumption was that, because data is produced through
time, the Zarr dataset would also be chunked in time. But the way a dataset is
written has a great impact on the performances of a read operation. For
instance, for a 3-dimension dataset (latitude, longitude and time) that is
written with no chunk in space and small chunks in time, getting the data for
the whole globe and a short period of time will be quite quick, but it will be
inefficient for a small region of the world and a long period of time. This is
because for the latter, a lot of files will have to be read, and most data in
each file will be thrown away. For this read operation to be efficient, we need
the dataset to be organized differently from the beginning: it must be written
with small chunks in space and big chunks (or no chunk at all) in time. In this
post, we will show how to transform a dataset that is chunked in time into a
dataset that is chunked in space (chunk transposition).

## Live data is already chunked in time


When we go to the FTP server of a data provider, we usually see a directory
structure that is organized in time. For instance, for NASA's GPM FTP server,
we have one directory per month:
```
200006/
200007/
200008/
...
```
And then for each directory, files corresponding to sub-periods of time. For
the `200006` directory, we have the following HDF5 files:
```
200006/3B-HHR-E.MS.MRG.3IMERG.20000601-S000000-E002959.0000.V06B.RT-H5
200006/3B-HHR-E.MS.MRG.3IMERG.20000601-S003000-E005959.0030.V06B.RT-H5
200006/3B-HHR-E.MS.MRG.3IMERG.20000601-S010000-E012959.0060.V06B.RT-H5
...
```
In this case, each file consists of a snapshot for the whole globe and a
30-minute period of time. As time goes by, new files become available on the
server. This way of producing a new file every 30 minutes implies that data is
inherently chunked in time from the start. In the [previous
post](https://medium.com/pangeo/continuously-extending-zarr-datasets-c54fbad3967d),
we basically kept this organization when saving the dataset to a Zarr archive
(we just made the chunks bigger for performance reasons). But now, we want to
incrementally build a Zarr archive that is chunked in space instead of chunked
in time.

Data can be rechunked using Xarray, but it turns out that [it doesn't work for
our particular
case](https://github.com/dask/dask/issues/5105#issuecomment-528084859) as of
today (although it could become possible in the future). Fortunately, we can
once again hack the Zarr archive to achieve our goal.

## Merging time chunks

For the time-chunked dataset to be transformed into a space-chunked dataset, we
need two things:

- chunk the dataset in space: this is easily done using Xarray's `chunk` method.
- unchunk the dataset in time: this can be done just by concatenating the chunk
  files (using e.g. `cat`).

Concatenating chunk files along the time dimension requires a bit of
bookkeeping, because it has to be done for each space chunk. Also, some
compression algorithms don't allow file concatenation, but GZip does.
Furthermore, if we keep on concatenating files, we will end up with the whole
dataset staged on our hard drive before uploading it to the cloud. With the ever
growing size of datasets, this might not become possible in the future. A
solution is to do a first concatenation locally, upload the pre-concatenated
chunks, and concatenate further in the cloud. Google Cloud Storage allows doing
that by "composing" objects.

In the end, here is the time-chunked dataset (for a 2-month period of time):
```
<xarray.Dataset>
Dimensions:                         (lat: 1800, lon: 3600, time: 2880)
Coordinates:
  * lat                             (lat) float32 -89.95 -89.85 ... 89.85 89.95
  * lon                             (lon) float32 -179.95 -179.85 ... 179.95
  * time                            (time) datetime64[ns] 2000-06-01 ... 2000-07-30T23:30:00
Data variables:
    precipitationCal                (time, lat, lon) float32 dask.array<shape=(2880, 1800, 3600), chunksize=(4, 1800, 3600)>
    probabilityLiquidPrecipitation  (time, lat, lon) float32 dask.array<shape=(2880, 1800, 3600), chunksize=(4, 1800, 3600)>
```

And the space-chunked dataset:
```
Dimensions:                         (lat: 1800, lon: 3600, time: 2880)
Coordinates:
  * lat                             (lat) float32 -89.95 -89.85 ... 89.85 89.95
  * lon                             (lon) float32 -179.95 -179.85 ... 179.95
  * time                            (time) datetime64[ns] 2000-06-01 ... 2000-07-30T23:30:00
Data variables:
    precipitationCal                (time, lat, lon) float32 dask.array<shape=(2880, 1800, 3600), chunksize=(2880, 100, 100)>
    probabilityLiquidPrecipitation  (time, lat, lon) float32 dask.array<shape=(2880, 1800, 3600), chunksize=(2880, 100, 100)>
```

As we can see, the first one is chunked in time with a size of 4 and not chunked
in space, and the second one is chunked in space with a size of 100x100 and not
chunked in time. This is not a binary choice, i.e. we might have to chunk in
space *and* time to get chunks that are of reasonable size (~100MiB), or even to
get a trade-off between the two extreme chunking schemes, thus allowing to store
only one version of the dataset. In that case, it won't be optimal but it will
work reasonably for most kind of analyses.

## Conclusion

The chunking scheme of a dataset can greatly impact the performances of
analyses. The same dataset, made available with different chunk shapes, can
better serve specific needs, by trading CPU ressources with storage space. In
this blog post, we showed a way to efficiently "transpose chunks" of a dataset
from time to space. This approach has been applied to upload the GPM IMERG
dataset to Pangeo. The same dataset is now available in two flavors:

- `gs://pangeo-data/gpm_imerg/early/chunk_time` for the time-chunked dataset.
  This version is optimal for analyses that need data over wide regions.
- `gs://pangeo-data/gpm_imerg/early/chunk_space` for the space-chunked dataset.
  That version is optimal for analyses that need data over long period of time.
