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
dt0 = datetime(2000, 6, 1) # upload from this date (ignore if resuming upload)
dt1 = datetime(2000, 7, 1) # upload up to this date (excluded)
wd = '.'

class State(object):
    def __init__(self, dt0, dt1):
        self.dt = dt0
        self.dt1 = dt1
        self.date_nb = 4 # corresponds to 2 hours
        self.download_dt = dt0
        self.download_filenames = []
        self.download_datetimes = []
        self.download_done = False
        self.chunk_space_date_i = 0
        self.chunk_space_date_nb = self.date_nb * 12 * 60 # concatenate in GCS every 30 days (this is a time-consuming operation)
        self.chunk_space_first_time = True
        self.chunk_space_date_nb_gcs = self.date_nb * 12 * 60 # time chunk is 60 days
        self.chunk_space_date_i_gcs = 0
        self.chunk_space_new_gcs_chunk = True
        self.gcs_chunk_space_exists = False
        self.pix_nb = 100

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
        with open(f'{wd}/gpm_imerg/tmp/gpm_list.txt', 'w') as f:
            f.write('\n'.join(urls))
        print(f'Downloading {state.date_nb} files from FTP...')
        done1 = False
        while not done1:
            p = subprocess.Popen(f'aria2c -x 4 -i {wd}/gpm_imerg/tmp/gpm_list.txt -d {wd}/gpm_imerg/tmp/gpm_data --ftp-user={login} --ftp-passwd={login} --continue=true'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
                    f = h5py.File(f'{wd}/gpm_imerg/tmp/gpm_data/{fname}', 'r')
                    last_ds = xr.Dataset({field: (['lon', 'lat'], f[f'Grid/{field}'][0].astype('float32')) for field in fields}, coords={'lon':f['Grid/lon'], 'lat':f['Grid/lat']}).transpose()
                    ds.append(last_ds)
                    f.close()
                except:
                    ds.append(last_ds)
                os.remove(f'{wd}/gpm_imerg/tmp/gpm_data/{fname}')
            ds = xr.concat(ds, 'time')
            ds = ds.assign_coords(time=state.download_datetimes[:state.date_nb])
            ds = ds.where(ds >= 0)
            await create_zarr(state, ds)
            state.download_filenames = state.download_filenames[state.date_nb:]
            state.download_datetimes = state.download_datetimes[state.date_nb:]
            with open(f'{wd}/gpm_imerg/tmp/state.pkl', 'wb') as f:
                pickle.dump(state, f)

async def create_zarr(state, ds):
    state.chunk_space_date_i += state.date_nb
    state.dt += timedelta(minutes=30*state.date_nb)
    i0 = state.chunk_space_date_i_gcs // state.chunk_space_date_nb_gcs
    i1 = i0 + 1
    p_copy_chunk_time = []
    if not os.path.exists(f'{wd}/gpm_imerg/early/chunk_time'):
        # chunk over time
        ds.chunk({'time': state.date_nb, 'lat': 1800, 'lon': 3600}).to_zarr(f'{wd}/gpm_imerg/early/chunk_time', encoding=encoding)
        print('Copying chunk_time to GCS...')
        p_copy_chunk_time.append(subprocess.Popen(f'gsutil -m cp -r {wd}/gpm_imerg/early/chunk_time gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        # chunk over space
        ds.chunk({'time': state.date_nb, 'lat': state.pix_nb, 'lon': state.pix_nb}).to_zarr(f'{wd}/gpm_imerg/early/chunk_space', encoding=encoding)
        subprocess.check_call(f'cp -r {wd}/gpm_imerg/early/chunk_space {wd}/gpm_imerg/early_stage/'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for field in fields:
            subprocess.check_call(f'rm {wd}/gpm_imerg/early/chunk_space/{field}/0.*', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        # chunk over time
        for dname in [f for f in os.listdir(f'{wd}/gpm_imerg/early/chunk_time') if (not f.startswith('.')) and (f != 'time')]:
            for fname in [f for f in os.listdir(f'{wd}/gpm_imerg/early/chunk_time/{dname}') if not f.startswith('.')]:
                os.remove(f'{wd}/gpm_imerg/early/chunk_time/{dname}/{fname}')
        ds.chunk({'time': state.date_nb, 'lat': 1800, 'lon': 3600}).to_zarr(f'{wd}/gpm_imerg/early/chunk_time', append_dim='time', mode='a')
        print('Copying chunk_time to GCS...')
        for field in fields:
            p_copy_chunk_time.append(subprocess.Popen(f'gsutil -m cp -r {wd}/gpm_imerg/early/chunk_time/{field} gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        # chunk over space
        ds.chunk({'time': state.date_nb, 'lat': state.pix_nb, 'lon': state.pix_nb}).to_zarr(f'{wd}/gpm_imerg/early/chunk_space', append_dim='time', mode='a')
        p_compose = []
        prefix_append = ''
        for field in fields:
            # concatenate locally early_stage 0.* and  early x.*
            for name_append in [f for f in os.listdir(f'{wd}/gpm_imerg/early/chunk_space/{field}') if not f.startswith('.')]:
                if not prefix_append:
                    # find index of chunk name
                    prefix_append = name_append[:name_append.find('.')]
                    if prefix_append == '0':
                        break
                    print(f'Concatenating locally 0.* and {prefix_append}.*')
                name = '0' + name_append[len(prefix_append):]
                path1 = f'{wd}/gpm_imerg/early_stage/chunk_space/{field}/{name}'
                path2 = f'{wd}/gpm_imerg/early/chunk_space/{field}/{name_append}'
                with open(path1, 'ab') as f1, open(path2, 'rb') as f2:
                    f1.write(f2.read())
                os.remove(path2)
            # set time chunks to time shape
            with open(f'{wd}/gpm_imerg/early/chunk_space/{field}/.zarray') as f1, open(f'{wd}/gpm_imerg/early_stage/chunk_space/{field}/.zarray', 'wt') as f2:
                zarray = json.load(f1)
                zarray['chunks'][0] = state.chunk_space_date_nb_gcs
                json.dump(zarray, f2)
            if (state.chunk_space_date_i == state.chunk_space_date_nb) or (state.dt == state.dt1):
                # rename 0.* to x.* locally
                if state.chunk_space_new_gcs_chunk:
                    i2 = i0
                else:
                    i2 = i1
                for prev_name in [f for f in os.listdir(f'{wd}/gpm_imerg/early_stage/chunk_space/{field}') if f.startswith('0.')]:
                    new_name = f'{i2}.{prev_name[2:]}'
                    path1 = f'{wd}/gpm_imerg/early_stage/chunk_space/{field}/{prev_name}'
                    path2 = f'{wd}/gpm_imerg/early_stage/chunk_space/{field}/{new_name}'
                    os.rename(path1, path2)
                # copy all x.* files to GCS
                print(f'Copying chunk_space/{field}/{i2}.* to GCS...')
                if state.gcs_chunk_space_exists:
                    subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/early_stage/chunk_space/{field} gs://pangeo-data/gpm_imerg/early/chunk_space'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/early_stage/chunk_space gs://pangeo-data/gpm_imerg/early'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    state.gcs_chunk_space_exists = True
                if not state.chunk_space_new_gcs_chunk:
                    # we need to concatenate in GCS (compose)
                    print(f'Concatenating chunk_space/{field}/{i0}.* and chunk_space/{field}/{i1}.* in GCS...')
                    for name_append in [f for f in os.listdir(f'{wd}/gpm_imerg/early_stage/chunk_space/{field}') if f.startswith(f'{i1}.')]:
                        name = f'{i0}.{name_append[2:]}'
                        path1 = f'gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/{name}'
                        path2 = f'gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/{name_append}'
                        cmd = f'gsutil compose {path1} {path2} {path1}'
                        p_compose.append(subprocess.Popen(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                subprocess.check_call(f'rm -rf {wd}/gpm_imerg/early_stage/chunk_space/{field}/{i2}.*', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if p_compose:
            await wait_for_process(p_compose)
            for field in fields:
                print(f'Cleaning chunk_space/{field} in GCS...')
                subprocess.check_call(f'gsutil -m rm -r gs://pangeo-data/gpm_imerg/early/chunk_space/{field}/{i1}.*'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await wait_for_process(p_copy_chunk_time)
    if state.chunk_space_date_i == state.chunk_space_date_nb:
        state.chunk_space_new_gcs_chunk = False
    state.chunk_space_date_i %= state.chunk_space_date_nb
    state.chunk_space_date_i_gcs += state.date_nb
    if (state.chunk_space_date_i_gcs % state.chunk_space_date_nb_gcs) == 0:
        state.chunk_space_new_gcs_chunk = True

async def wait_for_process(procs):
    if procs:
        message_printed = True#False
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

async def main():
    if resume_upload:
        with open(f'{wd}/gpm_imerg/tmp/state.pkl', 'rb') as f:
            state = pickle.load(f)
        state.dt1 = dt1
        state.download_done = False
    else:
        state = State(dt0, dt1)

    tasks = []
    tasks.append(download_files(state))
    tasks.append(process_files(state))

    await asyncio.gather(*tasks)

if not resume_upload:
    print('Cleaning in GCS...')
    try:
        subprocess.check_call('gsutil -m rm -rf gs://pangeo-data/gpm_imerg/early'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        pass
    shutil.rmtree(f'{wd}/gpm_imerg/early', ignore_errors=True)
    shutil.rmtree(f'{wd}/gpm_imerg/early_stage', ignore_errors=True)
    #gcloud init
    #gcloud auth login

login = os.getenv('GPM_LOGIN')
#fields = ['precipitationCal', 'precipitationUncal', 'randomError', 'HQprecipitation', 'HQprecipSource', 'HQobservationTime', 'IRprecipitation', 'IRkalmanFilterWeight', 'probabilityLiquidPrecipitation', 'precipitationQualityIndex']
fields = ['precipitationCal', 'probabilityLiquidPrecipitation']
shutil.rmtree(f'{wd}/gpm_imerg/tmp/gpm_data', ignore_errors=True)
os.makedirs(f'{wd}/gpm_imerg/tmp/gpm_data', exist_ok=True)
os.makedirs(f'{wd}/gpm_imerg/early', exist_ok=True)
os.makedirs(f'{wd}/gpm_imerg/early_stage', exist_ok=True)

compressor = GZip(level=1)
encoding = {field: {'compressor': compressor} for field in fields}

asyncio.run(main())

print('Finalizing')
ds = xr.open_zarr(f'{wd}/gpm_imerg/early/chunk_time')
ds2 = xr.DataArray(np.zeros(ds.time.shape), coords=[ds.time.values], dims=['time'])
shutil.rmtree(f'{wd}/gpm_imerg/early/chunk_time2', ignore_errors=True)
ds2.to_dataset(name='dummy').to_zarr(f'{wd}/gpm_imerg/early/chunk_time2')
# set time chunks to time shape
subprocess.check_call('gsutil -m rm -r gs://pangeo-data/gpm_imerg/early/chunk_time/time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/early/chunk_time2/time gs://pangeo-data/gpm_imerg/early/chunk_time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call('gsutil -m rm -r gs://pangeo-data/gpm_imerg/early/chunk_space/time'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.check_call(f'gsutil -m cp -r {wd}/gpm_imerg/early/chunk_time2/time gs://pangeo-data/gpm_imerg/early/chunk_space'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for field in fields:
    subprocess.check_call(f'gsutil cp {wd}/gpm_imerg/early_stage/chunk_space/{field}/.zarray gs://pangeo-data/gpm_imerg/early/chunk_space/{field}'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
