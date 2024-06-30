from pyproj import Transformer
from sgp4.api import Satrec, WGS84, jday
from datetime import datetime
import pyproj

class SatelliteInfo:
    def __init__(self, name, line_1, line_2):
        self.name = name
        self.satrec = Satrec.twoline2rv(line_1, line_2, WGS84)
        self.line_1 = line_1
        self.line_2 = line_2

    def cal_pos_vel(self, curr_time: datetime):
        jd, fr = jday(curr_time.year, curr_time.month, curr_time.day, curr_time.hour, curr_time.minute, curr_time.second)
        err, pos, vel = self.satrec.sgp4(jd, fr)
        if err != 0:
            raise RuntimeError(f'Error Propagating satellite {self.name}: {err}')
        return pos,vel


# converting data to lat-long
@DeprecationWarning  # pip install Deprecated
def ecef2lla(pos_x, pos_y, pos_z):
    ecef = pyproj.Proj(proj="geocent", ellps="WGS84", datum="WGS84")
    lla = pyproj.Proj(proj="latlong", ellps="WGS84", datum="WGS84")
    lon, lat, alt = pyproj.transform(ecef, lla, pos_x, pos_y, pos_z, radians=False)

    return lon, lat, alt


# info : https://pyproj4.github.io/pyproj/stable/examples.html#examples
# EPSG:4978 -> ECEF Coordinate System
# "EPSG:4979" -> World Geodetic System 1984 (WGS84) ellipsoid with latitude, longitude, and altitude.
def ecef2lla_ver2(pos):
    # Bug Fix: None can also be returned.
    if pos is None or any(p is None for p in pos):
        return [None, None, None]
    transformer = Transformer.from_crs("EPSG:4978", "EPSG:4979")
    lon, lat, alt = transformer.transform(pos[0], pos[1], pos[2], radians=False)
    return [lon, lat, alt]