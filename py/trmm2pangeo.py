from datetime import datetime, timedelta
import xarray as xr
import numpy as np
import zarr
import os
import shutil
import time
import subprocess
import gzip
import pickle

# set resume_upload=True when resuming an upload (must be False for the first upload).
# dt0 is the initial date of the dataset and must remain constant between uploads.
# dt1 is the date up to which you want to upload (excluded), and has to be increased between uploads.

dt0 = datetime(2000, 3, 1, 12) # DO NOT CHANGE (must stay constant between uploads)
#dt1 = datetime(2014, 4, 26, 0) # upload up to this date (excluded)
dt1 = datetime(2014, 4, 22, 12) # upload up to this date (excluded)
resume_upload = False

if resume_upload:
    with open('tmp/dt_trmm.pkl', 'rb') as f:
        dt = pickle.load(f)
else:
    dt = dt0
    shutil.rmtree('trmm_3b42rt', ignore_errors=True)
date_nb = 40
shutil.rmtree('tmp/trmm_data', ignore_errors=True)
shutil.rmtree('tmp/trmm_new', ignore_errors=True)
os.makedirs('tmp/trmm_data', exist_ok=True)
def download_files(dt, date_nb):
    datetimes = [dt + timedelta(hours=3*i) for i in range(date_nb)]
    urls, filenames = [], []
    for t in datetimes:
        year = t.year
        month = str(t.month).zfill(2)
        day = str(t.day).zfill(2)
        hour = str(t.hour).zfill(2)
        if t < datetime(2012, 11, 7, 6):
            filename = f'3B42RT.{year}{month}{day}{hour}.7R2.bin.gz'
        else:
            filename = f'3B42RT.{year}{month}{day}{hour}.7.bin.gz'
        urls.append(f'ftp://trmmopen.gsfc.nasa.gov/pub/merged/3B42RT/{year}/{month}/{filename}')
        filenames.append(filename)
    with open('tmp/trmm_list.txt', 'w') as f:
        f.write('\n'.join(urls))
    p = subprocess.Popen(f'aria2c -x 4 -i tmp/trmm_list.txt -d tmp/trmm_data --continue=true'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return p, filenames, datetimes, dt

first_time = True
while dt < dt1:
    if first_time:
        first_time = False
        p, filenames, datetimes, download_from_dt = download_files(dt, date_nb)
    new_time = datetimes
    waiting = False
    done = False
    while not done:
        return_code = p.poll()
        if return_code is None:
            if not waiting:
                t0 = time.time()
                waiting = True
            time.sleep(0.2)
        elif return_code != 0:
            print('Waiting for 3 hours...')
            time.sleep(3600*3) # 3 hours
            p, _, _, _ = download_files(download_from_dt, date_nb)
        else:
            done = True
    if waiting:
        t1 = time.time()
        print(f'Waited {round(t1-t0, 1)} seconds for download')
    print('\n'.join([str(i) for i in datetimes]))
    ds = []
    for fname in filenames:
        try:
            f = gzip.open(f'tmp/trmm_data/{fname}', 'rb')
            data = f.read()
            f.close()
            header_size_id = 'header_byte_length='
            header_size_pos = data[:1000].decode('utf-8').find(header_size_id) + len(header_size_id)
            header_size = int(data[header_size_pos:data[:1000].decode('utf-8').find(' ', header_size_pos)])
            header = data[:header_size]
            data = data[header_size:header_size + 480*1440*2]
            data = np.frombuffer(data, dtype=np.dtype('>i2'), count=480*1440)
            data_array = data.reshape((480, 1440)).astype(np.float32)/100 # in mm/h
            np.clip(data_array, 0, np.inf, out=data_array)
            lat = np.arange(60-0.25/2, -60, -0.25)
            lon = np.arange(0+0.25/2, 360, 0.25)
            last_ds = xr.Dataset({'precipitation': (['lat', 'lon'], data_array)}, coords={'lat':lat, 'lon':lon})
            ds.append(last_ds)
        except:
            ds.append(last_ds)
    shutil.rmtree('tmp/trmm_data', ignore_errors=True)
    os.makedirs('tmp/trmm_data', exist_ok=True)
    next_dt = dt + timedelta(hours=3*date_nb)
    p, next_filenames, next_datetimes, download_from_dt = download_files(next_dt, date_nb)
    ds = xr.concat(ds, 'time')
    ds = ds.assign_coords(time=new_time)
    if os.path.exists('trmm_3b42rt'):
        ds.to_zarr('tmp/trmm_new')
        for dname in [f for f in os.listdir('trmm_3b42rt') if not f.startswith('.')]:
            for fname in [f for f in os.listdir(f'trmm_3b42rt/{dname}') if not f.startswith('.')]:
                os.remove(f'trmm_3b42rt/{dname}/{fname}')
        trmm = zarr.open('trmm_3b42rt', mode='a')
        trmm_new = zarr.open('tmp/trmm_new', mode='r')
        for key in [k for k in trmm.array_keys() if k not in ['lat', 'lon']]:
            trmm[key].append(trmm_new[key])
        trmm['time'][-date_nb:] = zarr.array([i - dt0 for i in new_time], dtype='m8[h]')
        shutil.rmtree('tmp/trmm_new', ignore_errors=True)
        subprocess.check_call('gsutil -m cp -r trmm_3b42rt/ gs://pangeo-data/'.split())
        #subprocess.check_call('cp -r trmm_3b42rt/* trmm_bucket/', shell=True)
    else:
        ds = ds.chunk({'time': 40, 'lat': 480, 'lon': 1440})
        ds.to_zarr('trmm_3b42rt')
        subprocess.check_call('gsutil -m cp -r trmm_3b42rt/ gs://pangeo-data/'.split())
        #subprocess.check_call('cp -r trmm_3b42rt/ trmm_bucket/'.split())
    dt, filenames, datetimes = next_dt, next_filenames, next_datetimes
    with open('tmp/dt_trmm.pkl', 'wb') as f:
        pickle.dump(dt, f)
