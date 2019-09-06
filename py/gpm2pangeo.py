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
import json
import asyncio
from numcodecs import GZip

# set resume_upload=True when resuming an upload (must be False for the first upload).
# dt0 is the initial date of the dataset.
# dt1 is the date up to which you want to upload (excluded), and has to be increased between uploads.
# set environment variable GPM_LOGIN

resume_upload = False
dt0 = datetime(2000, 6, 1) # upload from this date
dt1 = datetime(2000, 7, 1) # upload up to this date (excluded)

if not resume_upload:
    print(f'Cleaning in GCS...')
    try:
        p = subprocess.check_call('gsutil -m rm -rf gs://pangeo-data/gpm_imerg/early'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        pass
    shutil.rmtree('gpm_imerg/early', ignore_errors=True)
    #gcloud init
    #gcloud auth login
date_nb = 4 # corresponds to 2 hours
chunk_space_date_nb = date_nb * 12 * 30 # concatenate in GCS every 30 days (this is a time-consuming operation)
login = os.getenv('GPM_LOGIN')
#fields = ['precipitationCal', 'precipitationUncal', 'randomError', 'HQprecipitation', 'HQprecipSource', 'HQobservationTime', 'IRprecipitation', 'IRkalmanFilterWeight', 'probabilityLiquidPrecipitation', 'precipitationQualityIndex']
fields = ['precipitationCal', 'probabilityLiquidPrecipitation']
shutil.rmtree('tmp/gpm_data', ignore_errors=True)
os.makedirs('tmp/gpm_data', exist_ok=True)
os.makedirs('gpm_imerg/early_stage', exist_ok=True)

compressor = GZip(level=1)
encoding = {field: {'compressor': compressor} for field in fields}

class State(object):
    def __init__(self, dt0, dt1, date_nb, chunk_space_date_nb):
        self.dt = dt0
        self.dt1 = dt1
        self.date_nb = date_nb
        self.download_dt = dt0
        self.download_filenames = []
        self.download_datetimes = []
        self.download_done = False
        self.chunk_space_date_i = 0
        self.chunk_space_date_nb = chunk_space_date_nb
        self.chunk_space_first_time = True

async def download_files(state):
    while state.download_dt < state.dt1:
        # no need to download files at a faster rate than we are able to upload the to GCS
        while len(state.download_filenames) > 2 * state.date_nb:
            await asyncio.sleep(1)
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
            filename = f'3B-HHR-E.MS.MRG.3IMERG.{year}{month}{day}-S{hour}{min0}00-E{hour}{min1}59.{minutes}.V06B.RT-H5'
            urls.append(f'ftp://jsimpson.pps.eosdis.nasa.gov/NRTPUB/imerg/early/{year}{month}/{filename}')
            filenames.append(filename)
        with open('tmp/gpm_list.txt', 'w') as f:
            f.write('\n'.join(urls))
        print(f'Downloading {date_nb} files from FTP...')
        done1 = False
        while not done1:
            p = subprocess.Popen(f'aria2c -x 4 -i tmp/gpm_list.txt -d tmp/gpm_data --ftp-user={login} --ftp-passwd={login} --continue=true'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            done2 = False
            while not done2:
                return_code = p.poll()
                if return_code is None:
                    await asyncio.sleep(1)
                elif return_code != 0:
                    await asyncio.sleep(1800)
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
            await asyncio.sleep(1)
        else:
            # we have downloaded files to process
            print('Processing files:')
            print('\n'.join([str(dt) for dt in state.download_datetimes]))
            ds = []
            for fname in state.download_filenames[:state.date_nb]:
                try:
                    f = h5py.File(f'tmp/gpm_data/{fname}', 'r')
                    last_ds = xr.Dataset({field: (['lon', 'lat'], f[f'Grid/{field}'][0]) for field in fields}, coords={'lon':f['Grid/lon'], 'lat':f['Grid/lat']}).transpose()
                    ds.append(last_ds)
                    f.close()
                except:
                    ds.append(last_ds)
                os.remove(f'tmp/gpm_data/{fname}')
            ds = xr.concat(ds, 'time')
            ds = ds.assign_coords(time=state.download_datetimes[:state.date_nb])
            ds = ds.where(ds >= 0)
            state.ds = ds
            await create_zarr(state)
            state.download_filenames = state.download_filenames[state.date_nb:]
            state.download_datetimes = state.download_datetimes[state.date_nb:]
            state.dt += timedelta(minutes=30*state.date_nb)
            with open('tmp/state.pkl', 'wb') as f:
                pickle.dump(state, f)

async def create_zarr(state):
    state.chunk_space_date_i += state.date_nb
    if not os.path.exists('gpm_imerg/early/chunk_time'):
        # chunk over time
        state.ds.chunk({'time': state.date_nb, 'lat': 1800, 'lon': 3600}).to_zarr('gpm_imerg/early/chunk_time', encoding=encoding)
        print('Copying chunk_time to GCS...')
        subprocess.check_call('gsutil -m cp -r gpm_imerg/early/chunk_time gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # chunk over space
        state.ds.chunk({'time': state.date_nb, 'lat': 100, 'lon': 100}).to_zarr('gpm_imerg/early/chunk_space', encoding=encoding)
        subprocess.check_call('cp -r gpm_imerg/early/chunk_space gpm_imerg/early_stage/'.split())
    else:
        # chunk over time
        for dname in [f for f in os.listdir('gpm_imerg/early/chunk_time') if (not f.startswith('.')) and (f != 'time')]:
            for fname in [f for f in os.listdir(f'gpm_imerg/early/chunk_time/{dname}') if not f.startswith('.')]:
                os.remove(f'gpm_imerg/early/chunk_time/{dname}/{fname}')
        state.ds.chunk({'time': state.date_nb, 'lat': 1800, 'lon': 3600}).to_zarr('gpm_imerg/early/chunk_time', append_dim='time', mode='a')
        for field in fields:
            print(f'Copying chunk_time/{field} to GCS...')
            subprocess.check_call(f'gsutil -m cp -r gpm_imerg/early/chunk_time/{field} gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # chunk over space
        for dname in [f for f in os.listdir('gpm_imerg/early/chunk_space') if (not f.startswith('.')) and (f != 'time')]:
            for fname in [f for f in os.listdir(f'gpm_imerg/early/chunk_space/{dname}') if not f.startswith('.')]:
                os.remove(f'gpm_imerg/early/chunk_space/{dname}/{fname}')
        state.ds.chunk({'time': state.date_nb, 'lat': 100, 'lon': 100}).to_zarr('gpm_imerg/early/chunk_space', append_dim='time', mode='a')
        for field in fields + ['time']:
            subprocess.check_call(f'cp -r gpm_imerg/early/chunk_space/{field}/* gpm_imerg/early_stage/chunk_space/{field}', shell=True)
            subprocess.check_call(f'cp -r gpm_imerg/early/chunk_space/{field}/.z* gpm_imerg/early_stage/chunk_space/{field}', shell=True)
        procs = []
        prefix_append = ''
        for field in fields:
            # concatenate locally
            for name_append in [f for f in os.listdir(f'gpm_imerg/early/chunk_space/{field}') if not f.startswith('.')]:
                if not prefix_append:
                    prefix_append = name_append[:name_append.find('.')]
                    print(f'Concatenating locally 0.* with {prefix_append}.*')
                name = name_append[len(prefix_append):]
                path1 = f'gpm_imerg/early_stage/chunk_space/{field}/0{name}'
                path2 = f'gpm_imerg/early_stage/chunk_space/{field}/{name_append}'
                with open(path1, 'ab') as f1, open(path2, 'rb') as f2:
                    f1.write(f2.read())
                os.remove(path2)
            # set time chunks to time shape
            with open(f'gpm_imerg/early_stage/chunk_space/{field}/.zarray') as f:
                zarray = json.load(f)
            zarray['chunks'][0] = zarray['shape'][0]
            with open(f'gpm_imerg/early_stage/chunk_space/{field}/.zarray', 'wt') as f:
                json.dump(zarray, f)
            if (not state.chunk_space_first_time) and (state.chunk_space_date_i == state.chunk_space_date_nb):
                # we need to concatenate in GCS
                # first, rename 0.* to 1.* locally
                for name_append in [f for f in os.listdir(f'gpm_imerg/early_stage/chunk_space/{field}') if f.startswith('0.')]:
                    name = '1.' + name_append[2:]
                    path1 = f'gpm_imerg/early_stage/chunk_space/{field}/{name_append}'
                    path2 = f'gpm_imerg/early_stage/chunk_space/{field}/{name}'
                    #subprocess.check_call(f'mv {path1} {path2}'.split())
                    os.rename(path1, path2)
                # then, copy all 1.* files to GCS
                print(f'Copying chunk_space/{field} to GCS...')
                subprocess.check_call(f'gsutil -m cp -r gpm_imerg/early_stage/chunk_space/{field} gs://pangeo-data/gpm_imerg/early/chunk_space'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                # last, concatenate in GCS (composing)
                print(f'Concatenating chunk_space/{field} in GCS...')
                for name_append in [f for f in os.listdir(f'gpm_imerg/early_stage/chunk_space/{field}') if f.startswith('1.')]:
                    name = '0.' + name_append[2:]
                    path1 = f'gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/{name}'
                    path2 = f'gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/{name_append}'
                    cmd = f'gsutil compose {path1} {path2} {path1}'
                    procs.append(subprocess.Popen(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        if state.chunk_space_first_time and (state.chunk_space_date_i == state.chunk_space_date_nb):
            state.chunk_space_first_time = False
            print('Copying chunk_space to GCS...')
            subprocess.check_call('gsutil -m cp -r gpm_imerg/early_stage/chunk_space gs://pangeo-data/gpm_imerg/early/chunk_space'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for field in fields:
                subprocess.check_call(f'rm -rf gpm_imerg/early_stage/chunk_space/{field}/0.*', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if procs:
            message_printed = False
            done = False
            while not done:
                done = True
                for proc in procs:
                    if proc.poll() is None:
                        if not message_printed:
                            print('Waiting for all processes to finish...')
                            message_printed = True
                        done = False
                        await asyncio.sleep(1)
                        break
            for field in fields:
                print(f'Cleaning chunk_space/{field} in GCS...')
                subprocess.check_call(f'gsutil -m rm gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/1.*'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for field in fields:
                subprocess.check_call(f'rm -rf gpm_imerg/early_stage/chunk_space/{field}/1.*', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    state.chunk_space_date_i %= state.chunk_space_date_nb

async def main():
    if resume_upload:
        with open('tmp/state.pkl', 'rb') as f:
            state = pickle.load(f)
        state.dt1 = dt1
        state.download_done = False
    else:
        state = State(dt0, dt1, date_nb, chunk_space_date_nb)

    tasks = []
    tasks.append(download_files(state))
    tasks.append(process_files(state))

    await asyncio.gather(*tasks)

asyncio.run(main())

ds = xr.open_zarr('gpm_imerg/early/chunk_time')
ds2 = xr.DataArray(np.zeros(ds.time.shape), coords=[ds.time.values], dims=['time'])
shutil.rmtree('gpm_imerg/early/chunk_time2', ignore_errors=True)
ds2.to_dataset(name='dummy').to_zarr('gpm_imerg/early/chunk_time2')
# set time chunks to time shape
subprocess.check_call('gsutil -m rm -r gs://pangeo-data/gpm_imerg/early/chunk_time/time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call('gsutil -m cp -r gpm_imerg/early/chunk_time2/time gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call('gsutil -m rm -r gs://pangeo-data/gpm_imerg/early/chunk_space/time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call('gsutil -m cp -r gpm_imerg/early/chunk_time2/time gs://pangeo-data/gpm_imerg/early/chunk_space'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for field in fields:
    subprocess.check_call(f'gsutil cp gpm_imerg/early_stage/chunk_space/{field}/.zarray gs://pangeo-data/gpm_imerg/early/chunk_space/{field}'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
