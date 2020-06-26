from datetime import datetime, timedelta
import xarray as xr
import zarr
import os
import sys
import shutil
import time
import subprocess
import h5py
import pickle
import numpy as np
import pandas as pd
import asyncio
import json

# set resume_upload=True when resuming an upload (must be False for the first upload).
# dt0 is the initial date of the dataset.
# dt1 is the date up to which you want to upload (excluded), and has to be increased between uploads.
# set environment variable GPM_LOGIN

resume_upload = False
resume_from = None # if not None (e.g. datetime(2002, 2, 20)), upload will be resumed from this date
dt0 = datetime(2000, 6, 1) # upload from this date (ignored if resume_upload==True)
dt1 = dt0 + timedelta(days=100) # upload up to this date (excluded)
wd = '.'
gcs = True  # if False, save locally instead of uploading to GCS

class State(object):
    def __init__(self, dt0, dt1):
        self.dt = dt0
        self.dt1 = dt1
        self.date_nb = 8 # corresponds to 4 hours
        self.download_dt = dt0
        self.download_filenames = []
        self.download_datetimes = []
        self.download_done = False
        self.chunk_time_date_i = 0
        self.fields = ['precipitationCal', 'precipitationUncal', 'randomError', 'HQprecipitation', 'HQprecipSource', 'HQobservationTime', 'IRprecipitation', 'IRkalmanFilterWeight', 'probabilityLiquidPrecipitation', 'precipitationQualityIndex']
        self.login = os.getenv('GPM_LOGIN')

def copytree(src, dst):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d)
        else:
            shutil.copy2(s, d)

def get_time_nb(da):
    # chunk in time to get ~100MB chunks
    dtype = str(da.dtype)
    if dtype == 'float32':
        time_nb = 4 # 4 * 1800 * 3600 * 4 / 1024 / 1024 = 99 MB
    elif dtype == 'int16':
        time_nb = 8 # 8 * 1800 * 3600 * 2 / 1024 / 1024 = 99 MB
    else:
        print(f'Wrong dtype {dtype} for {da.name}')
        sys.exit()
    return time_nb

async def download_files(state):
    while state.download_dt < state.dt1:
        # no need to download files at a faster rate than we are able to upload to GCS
        while len(state.download_filenames) > 3 * state.date_nb:
            await asyncio.sleep(0.1)
        datetimes = [state.download_dt + timedelta(minutes=30*i) for i in range(state.date_nb)]
        urls, filenames = [], []
        for t in datetimes:
            year = t.year
            month = str(t.month).zfill(2)
            day = str(t.day).zfill(2)
            hour = str(t.hour).zfill(2)
            min0 = str(t.minute).zfill(2)
            min1 = t.minute + 29
            minutes = str(t.hour*60+t.minute).zfill(4)
            filename = f'3B-HHR-L.MS.MRG.3IMERG.{year}{month}{day}-S{hour}{min0}00-E{hour}{min1}59.{minutes}.V06B.RT-H5'
            urls.append(f'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/{year}{month}/{filename}')
            filenames.append(filename)
        with open(f'{wd}/gpm_imerg/tmp/gpm_list.txt', 'w') as f:
            f.write('\n'.join(urls))
        print(f'Downloading {state.date_nb} files from FTP...')
        done1 = False
        while not done1:
            cmd = f'aria2c --http-auth-challenge=true --http-user={state.login} --http-passwd={state.login} -x 8 -i {wd}/gpm_imerg/tmp/gpm_list.txt -d {wd}/gpm_imerg/tmp/gpm_data --continue=true'
            p = subprocess.Popen(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            done2 = False
            while not done2:
                return_code = p.poll()
                if return_code is None:
                    await asyncio.sleep(0.1)
                elif return_code != 0:
                    await asyncio.sleep(1)
                    done2 = True
                else:
                    state.download_filenames += filenames
                    state.download_datetimes += datetimes
                    state.download_dt += timedelta(minutes=30*state.date_nb)
                    done1 = True
                    done2 = True
    state.download_done = True

async def process_files(state):
    while True:
        if state.download_done and (not state.download_filenames):
            # no more files to download and we processed all downloaded files, we are done
            return
        if not state.download_filenames:
            # wait for files to be downloaded
            await asyncio.sleep(0.1)
        else:
            # we have downloaded files to process
            print('Processing files:')
            print('\n'.join([str(dt) for dt in state.download_datetimes[:state.date_nb]]))
            ds = []
            for fname in state.download_filenames[:state.date_nb]:
                try:
                    f = h5py.File(f'{wd}/gpm_imerg/tmp/gpm_data/{fname}', 'r')
                    last_ds = xr.Dataset({field: (['lon', 'lat'], f[f'Grid/{field}'][0]) for field in state.fields}, coords={'lon':f['Grid/lon'], 'lat':f['Grid/lat']}).transpose()
                    ds.append(last_ds)
                    f.close()
                except:
                    ds.append(last_ds)
                os.remove(f'{wd}/gpm_imerg/tmp/gpm_data/{fname}')
            ds = xr.concat(ds, 'time')
            ds = ds.assign_coords(time=state.download_datetimes[:state.date_nb])
            await create_zarr(state, ds)
            state.download_filenames = state.download_filenames[state.date_nb:]
            state.download_datetimes = state.download_datetimes[state.date_nb:]
            with open(f'{wd}/gpm_imerg/tmp/state.pkl', 'wb') as f:
                pickle.dump(state, f)

async def create_zarr(state, ds):
    state.dt += timedelta(minutes=30*state.date_nb)
    for field in state.fields:
        time_nb = get_time_nb(ds[field])
        ds[field] = ds[field].chunk({'time': time_nb, 'lat': 1800, 'lon': 3600})
    if not os.path.exists(f'{wd}/gpm_imerg/late/chunk_time'):
        ds.to_zarr(f'{wd}/gpm_imerg/late/chunk_time')
        print('Uploading chunk_time to GCS...')
        if gcs:
            subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/late/chunk_time gs://pangeo-data/gpm_imerg/late/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            shutil.copytree(f'{wd}/gpm_imerg/late/chunk_time', f'{wd}/gcs/gpm_imerg/late/chunk_time')
    else:
        shutil.rmtree(f'{wd}/gpm_imerg/late/chunk_time', ignore_errors=True)
        ds.to_zarr(f'{wd}/gpm_imerg/late/chunk_time')
        print('Uploading chunk_time to GCS...')
        for field in state.fields:
            time_nb = get_time_nb(ds[field])
            chunk_nb = 8 // time_nb
            for prev_name in [f for f in os.listdir(f'{wd}/gpm_imerg/late/chunk_time/{field}') if f[0].isdigit()]:
                chunk_i = int(prev_name[:prev_name.find('.')])
                new_name = f"{state.chunk_time_date_i*chunk_nb+chunk_i}{prev_name[prev_name.find('.'):]}"
                path1 = f'{wd}/gpm_imerg/late/chunk_time/{field}/{prev_name}'
                path2 = f'{wd}/gpm_imerg/late/chunk_time/{field}/{new_name}'
                os.rename(path1, path2)
            # set time time shape
            with open(f'{wd}/gpm_imerg/late/chunk_time/{field}/.zarray') as f:
                zarray = json.load(f)
            zarray['shape'][0] = (state.chunk_time_date_i + 1) * state.date_nb
            with open(f'{wd}/gpm_imerg/late/chunk_time/{field}/.zarray', 'wt') as f:
                json.dump(zarray, f)
            if gcs:
                done = False
                while not done:
                    done = True
                    try:
                        subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/late/chunk_time/{field} gs://pangeo-data/gpm_imerg/late/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    except:
                        done = False
            else:
                copytree(f'{wd}/gpm_imerg/late/chunk_time/{field}/', f'{wd}/gcs/gpm_imerg/late/chunk_time/{field}/')
            await asyncio.sleep(0)
    state.chunk_time_date_i += 1

async def main():
    shutil.rmtree(f'{wd}/gpm_imerg/tmp/gpm_data', ignore_errors=True)
    os.makedirs(f'{wd}/gpm_imerg/tmp/gpm_data')

    if resume_upload:
        with open(f'{wd}/gpm_imerg/tmp/state.pkl', 'rb') as f:
            state = pickle.load(f)
        state.dt1 = dt1
        state.download_done = False
        state.download_datetimes = []
        state.download_filenames = []
        if resume_from is not None:
            state.dt = resume_from
            state.download_dt = state.dt
        state.chunk_time_date_i = int((state.dt - dt0).total_seconds()) // 1800 // state.date_nb
    else:
        state = State(dt0, dt1)
        print('Cleaning in GCS...')
        if gcs:
            try:
                subprocess.check_call('gsutil -m rm -rf gs://pangeo-data/gpm_imerg/late'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except:
                pass
            #gcloud init
            #gcloud auth login
        else:
            shutil.rmtree(f'{wd}/gcs', ignore_errors=True)
            os.makedirs(f'{wd}/gcs/gpm_imerg/late')
        shutil.rmtree(f'{wd}/gpm_imerg/late', ignore_errors=True)
    os.makedirs(f'{wd}/gpm_imerg/late', exist_ok=True)

    tasks = []
    tasks.append(download_files(state))
    tasks.append(process_files(state))

    await asyncio.gather(*tasks)

    print('Finalizing')

    time_var = pd.date_range(dt0, dt1-timedelta(minutes=30), freq='30min') + timedelta(minutes=15)
    time_ds = xr.DataArray(np.zeros(len(time_var)), coords=[time_var], dims=['time']).to_dataset(name='gpm_time')
    shutil.rmtree(f'{wd}/gpm_imerg/late/time', ignore_errors=True)
    time_ds.to_zarr(f'{wd}/gpm_imerg/late/time')

    # set time chunks to time shape
    if gcs:
        done = False
        while not done:
            done = True
            try:
                subprocess.check_call('gsutil -m rm -r gs://pangeo-data/gpm_imerg/late/chunk_time/time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/late/time/time gs://pangeo-data/gpm_imerg/late/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except:
                done = False
    else:
        shutil.rmtree(f'{wd}/gcs/gpm_imerg/late/chunk_time/time', ignore_errors=True)
        shutil.copytree(f'{wd}/gpm_imerg/late/time/time', f'{wd}/gcs/gpm_imerg/late/chunk_time/time')

    print('Done')



asyncio.run(main())
