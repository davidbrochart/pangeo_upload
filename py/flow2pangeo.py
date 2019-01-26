import xarray as xr
import os
from osgeo import gdal
from tqdm import tqdm
import numpy as np

# first compute flow accumulation at 3-second resolution from
# https://github.com/davidbrochart/flow_acc_3s

# gdalbuildvrt needs same datatype but some flow direction tiles are of type
# byte and others are of type int16.
# we convert all tiles to uint8 and pad them to 6000x6000 arrays.
# cd tiles
tiles = [tile for tile in os.listdir('dir/3s') if tile.endswith('_dir')]
for tile in tqdm(tiles):
    adf = f'dir/3s/{tile}/{tile}/w001001.adf'
    tif = f'dir/3s/{tile}.tif'
    lat = int(tile[1:3])
    if tile[0] == 's':
        lat = -lat
    lat += 5
    lon = int(tile[4:7])
    if tile[3] == 'w':
        lon = -lon
    flow_dir = gdal.Open(adf)
    geo = flow_dir.GetGeoTransform()
    ySize, xSize = flow_dir.RasterYSize, flow_dir.RasterXSize
    a = flow_dir.ReadAsArray()
    a = np.where(a==-1, 255, a).astype(np.uint8)
    # data is padded into a 6000x6000 array (some tiles may be smaller):
    array_5x5 = np.ones((6000, 6000), dtype=np.uint8) * 255
    y0 = int(round((geo[3] - lat) / geo[5]))
    y1 = 6000 - int(round(((lat - 5) - (geo[3] + geo[5] * ySize)) / geo[5]))
    x0 = int(round((geo[0] - lon) / geo[1]))
    x1 = 6000 - int(round(((lon + 5) - (geo[0] + geo[1] * xSize)) / geo[1]))
    array_5x5[y0:y1, x0:x1] = a

    geo[3] = lat
    geo[0] = lon
    driver = gdal.GetDriverByName('GTiff')
    ds = driver.Create(tif, 6000, 6000, 1, gdal.GDT_Byte, ['COMPRESS=LZW'])
    ds.SetGeoTransform(geo)
    ds.SetProjection('GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433],AUTHORITY[\"EPSG\",\"4326\"]]')
    band = ds.GetRasterBand(1)
    band.WriteArray(array_5x5)
    band.SetNoDataValue(255)
    ds = None

# gdalbuildvrt dir.vrt dir/3s/*.tif
# gdalbuildvrt acc.vrt acc/3s/*.tif
# sed -i 's/relativeToVRT="1">/relativeToVRT="1">\/vsigs\/pangeo-data\/hydrosheds\//g' acc.vrt
# sed -i 's/relativeToVRT="1">/relativeToVRT="1">\/vsigs\/pangeo-data\/hydrosheds\//g' dir.vrt
# cd ..
# mv tiles hydrosheds
# gsutil -m cp -r hydrosheds gs://pangeo-data/
