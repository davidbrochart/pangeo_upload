import xarray as xr
import os
import shutil
import zarr
import subprocess

# go to https://cgiarcsi.community/data/global-aridity-and-pet-database/
# DATA -> GLOBAL ARIDITY AND PET DATABASE V2 -> global-et0_monthly.tif.zip
# (https://ndownloader.figshare.com/files/13901324)
# unzip global-et0_monthly.tif.zip
# cd et0_month

shutil.rmtree('cgiar_pet', ignore_errors=True)
shutil.rmtree('tmp/pet_new', ignore_errors=True)
os.makedirs('tmp', exist_ok=True)

for month0 in range(1, 13, 2):
    ds, new_month = [], []
    for month in range(month0, month0+2):
        ds.append(xr.Dataset({'PET': xr.open_rasterio(f'et0_{str(month).zfill(2)}.tif').sel(band=1).rename({'y': 'lat', 'x': 'lon'})}))
        new_month.append(month)
    ds = xr.concat(ds, 'month')
    ds = ds.assign_coords(month=new_month)
    ds = ds.drop('band')
    if os.path.exists('cgiar_pet'):
        ds.to_zarr('tmp/pet_new')
        pet = zarr.open('cgiar_pet', mode='a')
        pet_new = zarr.open('tmp/pet_new', mode='r')
        for key in [k for k in pet.array_keys() if k not in ['lat', 'lon']]:
            pet[key].append(pet_new[key])
        pet['month'][-2:] = zarr.array(new_month)
        shutil.rmtree('tmp/pet_new', ignore_errors=True)
    else:
        ds.to_zarr('cgiar_pet')

subprocess.check_call('gsutil -m cp -r cgiar_pet gs://pangeo-data/'.split())
