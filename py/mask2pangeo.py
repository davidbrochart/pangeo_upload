import xarray as xr
import numpy as np
import os
from tqdm import tqdm
import shutil
import subprocess

shutil.rmtree('tmp/ds_mask', ignore_errors=True)
shutil.rmtree('tmp/ds_mask_new', ignore_errors=True)
labels = os.listdir('ws_mask/amazonas')
arrays = {}
for label in tqdm(labels):
    ds = xr.open_zarr(f'ws_mask/amazonas/{label}')
    da = ds['mask'].compute()
    arrays[label] = da
array_dims = ['lat', 'lon']
concat_dim = 'label'
pix_deg = 1 / 1200
tolerance = 0.1 * pix_deg
vmin, vmax = {}, {}
for d in array_dims:
    vmin[d] = np.inf
    vmax[d] = -np.inf
for label in arrays:
    da = arrays[label]
    this_vmin, this_vmax = {}, {}
    for d in array_dims:
        vmin[d] = min(vmin[d], np.min(da[d]).values)
        vmax[d] = max(vmax[d], np.max(da[d]).values)
coord = {}
for d in array_dims:
    if arrays['0'][d].values[1] - arrays['0'][d].values[0] > 0: # increasing
        coord[d] = np.arange(vmin[d], vmax[d]+tolerance, pix_deg)
    else:
        coord[d] = np.arange(vmax[d], vmin[d]-tolerance, -pix_deg)
nlat = int(round((coord['lat'][0] - coord['lat'][-1]) / pix_deg)) + 1
nlon = int(round((coord['lon'][-1] - coord['lon'][0]) / pix_deg)) + 1
n = 2
for label in tqdm(arrays):
    da = arrays[label]
    a = np.zeros((nlat, nlon), dtype=np.uint8)
    dlat = int(round((coord['lat'][0] - da.lat.values[0]) / pix_deg))
    dlon = int(round((da.lon.values[0] - coord['lon'][0]) / pix_deg))
    a[dlat:dlat+da.shape[0], dlon:dlon+da.shape[1]] = da.values
    attrs = da.attrs
    attrs['bbox'] = ((da.lat.values[0]+pix_deg/2, da.lon.values[0]-pix_deg/2), (da.lat.values[-1]-pix_deg/2, da.lon.values[-1]+pix_deg/2))
    da_reindex = xr.DataArray(a, coords=[coord['lat'], coord['lon']], dims=['lat', 'lon'], attrs=attrs)
    ds = da_reindex.to_dataset(name=label)
    ds.to_zarr('tmp/ds_mask_new')
    if os.path.exists('tmp/ds_mask'):
        subprocess.check_call(f'cp -r tmp/ds_mask_new/{label} tmp/ds_mask/{label}'.split())
        shutil.rmtree('tmp/ds_mask_new', ignore_errors=True)
    else:
        os.rename('tmp/ds_mask_new', 'tmp/ds_mask')
subprocess.check_call('gsutil -m cp -r tmp/ds_mask/ gs://pangeo-data/amazonas/'.split())
