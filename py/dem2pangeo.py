import zipfile
import numpy as np
from pandas import DataFrame
from osgeo import gdal
import os
import subprocess
from tqdm import tqdm

os.makedirs('dem/3s', exist_ok=True)
os.makedirs('tmp', exist_ok=True)
subprocess.check_call('rm -rf dem/3s/*', shell=True)
subprocess.check_call('rm -rf tmp/*', shell=True)

with zipfile.ZipFile('/mnt/hgfs/SharedWithVM/CON_3s_GRID.zip', 'r') as z1:
    ziplist = z1.infolist()
    for zf in tqdm(ziplist):
        fpath = zf.filename
        if fpath.endswith('.zip'):
            tile = os.path.basename(fpath)[:-4].replace('con_grid', 'dem')
            lat = int(tile[1:3])
            if tile[0] == 's':
                lat = -lat
            lat += 5 # upper left
            lon = int(tile[4:7])
            if tile[3] == 'w':
                lon = -lon
            z1.extract(fpath, path='tmp')
            with zipfile.ZipFile(f'tmp/{fpath}', 'r') as z2:
                z2.extractall(path = 'tmp')
            for root, dirs, files in os.walk('tmp'):
                for fname in files:
                    if fname == 'w001001.adf':
                        dem_path = root + '/' + fname
            con_dem = gdal.Open(dem_path)
            geo = list(con_dem.GetGeoTransform())
            ySize, xSize = con_dem.RasterYSize, con_dem.RasterXSize
            con_dem = con_dem.ReadAsArray()
            subprocess.check_call('rm -rf tmp/*', shell=True)
            # data is padded into a 6000x6000 array (some tiles may be smaller):
            array_5x5 = np.ones((6000, 6000), dtype='int16') * -32768
            y0 = int(round((geo[3] - lat) / geo[5]))
            y1 = 6000 - int(round(((lat - 5) - (geo[3] + geo[5] * ySize)) / geo[5]))
            x0 = int(round((geo[0] - lon) / geo[1]))
            x1 = 6000 - int(round(((lon + 5) - (geo[0] + geo[1] * xSize)) / geo[1]))
            array_5x5[y0:y1, x0:x1] = con_dem

            tif = f'dem/3s/{tile}.tif'
            geo[3] = lat
            geo[0] = lon
            driver = gdal.GetDriverByName('GTiff')
            ds = driver.Create(tif, 6000, 6000, 1, gdal.GDT_Byte, ['COMPRESS=LZW'])
            ds.SetGeoTransform(geo)
            ds.SetProjection('GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433],AUTHORITY[\"EPSG\",\"4326\"]]')
            band = ds.GetRasterBand(1)
            band.WriteArray(array_5x5)
            band.SetNoDataValue(-32768)
            ds = None

# gdalbuildvrt dem.vrt dem/3s/*.tif
# sed -i 's/relativeToVRT="1">/relativeToVRT="1">\/vsigs\/pangeo-data\/hydrosheds\//g' dem.vrt
# gsutil -m cp dem.vrt gs://pangeo-data/hydrosheds/
# gsutil -m cp -r dem gs://pangeo-data/hydrosheds/
