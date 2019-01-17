These are scripts to upload data to `pangeo-data` bucket on Google Cloud
Storage. There are tw0 datasets which grow with time (GPM and TRMM), and the
scripts allow for incremental upload, so you can resume the upload when new data
is available. It is also convenient if you don't have a big hard drive, because
you don't have to stage the whole `zarr` archive before uploading.
