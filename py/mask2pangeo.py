import xarray as xr
import numpy as np
import os
from tqdm import tqdm
import shutil
import subprocess
import zarr

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)
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
max_len = max([len(label) for label in list(arrays)])
for label in tqdm(arrays):
    da = arrays[label]
    a = np.zeros((1, coord['lat'].shape[0], coord['lon'].shape[0]), dtype=np.uint8)
    dlat = int(round((coord['lat'][0] - da.lat.values[0]) / pix_deg))
    dlon = int(round((da.lon.values[0] - coord['lon'][0]) / pix_deg))
    a[0, dlat:dlat+da.shape[0], dlon:dlon+da.shape[1]] = da.values
    da_reindex = xr.DataArray(a, coords=[[label], coord['lat'], coord['lon']], dims=['label', 'lat', 'lon'])
    ds = da_reindex.to_dataset(name='mask')
    if os.path.exists('tmp/ds_mask'):
        shutil.rmtree('tmp/ds_mask_new', ignore_errors=True)
        ds.to_zarr('tmp/ds_mask_new', encoding={'label': {'dtype': f'|S{max_len}'}, 'mask': {'compressor': compressor}})
        ds_mask = zarr.open('tmp/ds_mask', mode='a')
        ds_new = zarr.open('tmp/ds_mask_new', mode='r')
        ds_mask['mask'].append(ds_new['mask'])
        ds_mask['label'].append(ds_new['label'])
    else:
        ds.to_zarr('tmp/ds_mask', encoding={'label': {'dtype': f'|S{max_len}'}, 'mask': {'compressor': compressor}})
subprocess.check_call('gsutil -m cp -r tmp/ds_mask/ gs://pangeo-data/amazonas/'.split())
