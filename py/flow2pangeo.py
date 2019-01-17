import xarray as xr
import subprocess

# compute flow accumulation at 3-second resolution from
# https://github.com/davidbrochart/flow_acc_3s
# gdalbuildvrt dir.vrt tiles/dir/3s/*/*/w001001.adf

acc = xr.open_rasterio('acc.vrt').sel(band=1).rename({'y': 'lat', 'x': 'lon'})
dir_ = xr.open_rasterio('dir.vrt').sel(band=1).rename({'y': 'lat', 'x': 'lon'})
ds = xr.Dataset({'acc': acc, 'dir': dir_}).drop('band')

ds.to_zarr('hydrosheds_3sec')

subprocess.check_call('gsutil -m cp -r hydrosheds_3sec gs://pangeo-data/'.split())
