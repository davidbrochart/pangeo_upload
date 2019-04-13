from datetime import datetime, timedelta
import xarray as xr
import zarr
import os
import shutil
import time
import subprocess
import h5py
import pickle

# set resume_upload=True when resuming an upload (must be False for the first upload).
# dt0 is the initial date of the dataset and must remain constant between uploads.
# dt1 is the date up to which you want to upload (excluded), and has to be increased between uploads.
# set environment variable GPM_LOGIN

dt0 = datetime(2014, 3, 12) # DO NOT CHANGE (must stay constant between uploads)
dt1 = datetime(2019, 4, 1) # upload up to this date (excluded)
resume_upload = False

if resume_upload:
    with open('tmp/dt_gpm.pkl', 'rb') as f:
        dt = pickle.load(f)
else:
    dt = dt0
    shutil.rmtree('gpm_imerg_early', ignore_errors=True)
    #gcloud init
    #gcloud auth login
date_nb = 4
login = os.getenv('GPM_LOGIN')
#fields = ['precipitationCal', 'precipitationUncal', 'randomError', 'HQprecipitation', 'HQprecipSource', 'HQobservationTime', 'IRprecipitation', 'IRkalmanFilterWeight', 'probabilityLiquidPrecipitation', 'precipitationQualityIndex']
fields = ['precipitationCal']
shutil.rmtree('tmp/gpm_data', ignore_errors=True)
shutil.rmtree('tmp/gpm_new', ignore_errors=True)
os.makedirs('tmp/gpm_data', exist_ok=True)
def download_files(dt, date_nb):
    datetimes = [dt + timedelta(minutes=30*i) for i in range(date_nb)]
    urls, filenames = [], []
    for t in datetimes:
        year = t.year
        month = str(t.month).zfill(2)
        day = str(t.day).zfill(2)
        hour = str(t.hour).zfill(2)
        min0 = str(t.minute).zfill(2)
        min1 = t.minute + 29
        minutes = str(t.hour*60+t.minute).zfill(4)
        filename = f'3B-HHR-E.MS.MRG.3IMERG.{year}{month}{day}-S{hour}{min0}00-E{hour}{min1}59.{minutes}.V05B.RT-H5'
        urls.append(f'ftp://jsimpson.pps.eosdis.nasa.gov/NRTPUB/imerg/early/{year}{month}/{filename}')
        filenames.append(filename)
    with open('tmp/gpm_list.txt', 'w') as f:
        f.write('\n'.join(urls))
    p = subprocess.Popen(f'aria2c -x 4 -i tmp/gpm_list.txt -d tmp/gpm_data --ftp-user={login} --ftp-passwd={login} --continue=true'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return p, filenames, datetimes, dt

first_time = True
while dt < dt1:
    if first_time:
        first_time = False
        p, filenames, datetimes, download_from_dt = download_files(dt, date_nb)
    new_time = [i + timedelta(minutes=15) for i in datetimes]
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
            time.sleep(1800) # 30 minutes
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
            f = h5py.File(f'tmp/gpm_data/{fname}', 'r')
            last_ds = xr.Dataset({field: (['lon', 'lat'], f[f'Grid/{field}']) for field in fields}, coords={'lon':f['Grid/lon'], 'lat':f['Grid/lat']}).transpose()
            ds.append(last_ds)
            f.close()
        except:
            ds.append(last_ds)
    shutil.rmtree('tmp/gpm_data', ignore_errors=True)
    os.makedirs('tmp/gpm_data', exist_ok=True)
    next_dt = dt + timedelta(minutes=30*date_nb)
    p, next_filenames, next_datetimes, download_from_dt = download_files(next_dt, date_nb)
    ds = xr.concat(ds, 'time')
    ds = ds.assign_coords(time=new_time)
    if os.path.exists('gpm_imerg_early'):
        ds.to_zarr('tmp/gpm_new')
        for dname in [f for f in os.listdir('gpm_imerg_early') if not f.startswith('.')]:
            for fname in [f for f in os.listdir(f'gpm_imerg_early/{dname}') if not f.startswith('.')]:
                os.remove(f'gpm_imerg_early/{dname}/{fname}')
        gpm = zarr.open('gpm_imerg_early', mode='a')
        gpm_new = zarr.open('tmp/gpm_new', mode='r')
        for key in [k for k in gpm.array_keys() if k not in ['lat', 'lon']]:
            gpm[key].append(gpm_new[key])
        gpm['time'][-date_nb:] = zarr.array([i - (dt0 + timedelta(minutes=15)) for i in new_time], dtype='m8[m]')
        shutil.rmtree('tmp/gpm_new', ignore_errors=True)
        subprocess.check_call('gsutil -m cp -r gpm_imerg_early/ gs://pangeo-data/'.split())
        #subprocess.check_call('cp -r gpm_imerg_early/* gpm_bucket/', shell=True)
    else:
        ds = ds.chunk({'time': 4, 'lat': 1800, 'lon': 3600})
        ds.to_zarr('gpm_imerg_early')
        subprocess.check_call('gsutil -m cp -r gpm_imerg_early/ gs://pangeo-data/'.split())
        #subprocess.check_call('cp -r gpm_imerg_early/ gpm_bucket/'.split())
    dt, filenames, datetimes = next_dt, next_filenames, next_datetimes
    with open('tmp/dt_gpm.pkl', 'wb') as f:
        pickle.dump(dt, f)
