from datetime import datetime, timedelta
from sgp4.api import Satrec, WGS84, jday
from pyproj import Transformer
import pyproj


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
def ecef2lla_ver2(pos_x, pos_y, pos_z):
    transformer = Transformer.from_crs("EPSG:4978", "EPSG:4979")
    lon, lat, alt = transformer.transform(pos_x, pos_y, pos_z, radians=False)

    return lon, lat, alt


class SatelliteInfo:
    def __init__(self, name, line_1, line_2):
        self.name = name
        self.satrec = Satrec.twoline2rv(line_1, line_2, WGS84)

    # DEBUG
    def get_name(self):
        return self.name

    # cal pos and velocity at any given time
    def cal_pos_vel(self, curr_time: datetime):
        # jd - Julian Date
        # fr - precision fraction
        jd, fr = jday(curr_time.year, curr_time.month, curr_time.day, curr_time.hour, curr_time.minute,
                      curr_time.second)
        err, pos, vel = self.satrec.sgp4(jd, fr)
        if err != 0:
            # raise error
            raise RuntimeError(f'Error propagating satellite {self.get_name()}: {err}')

        return pos, vel


class SatelliteTracker:
    def __init__(self, file_path):
        self.satellites = self.parse_file(file_path)

    # DEBUG
    def show_satellite_names(self):
        for sat in self.satellites:
            print(f'sat= {sat.get_name()}')

    # parsing the file
    def parse_file(self, file_path):
        satellites = []
        with open(file_path, 'r') as file:
            lines = file.readlines()
            size = len(lines)

            # line-1 : name of satellite
            # line-2 : line-1 (date-time details. etc.)
            # line-2 : line-2 (motion, inclination. etc.)

            for idx in range(0, size, 3):
                satellite_name = lines[idx].strip()
                line_1 = lines[idx + 1].strip()
                line_2 = lines[idx + 2].strip()

                satellites.append(SatelliteInfo(satellite_name, line_1, line_2))
        return satellites

    # Track The satellite
    # ## Error Codes
    # 1 - Mean eccentricity is outside the range 0 ≤ e < 1.
    # 2 - Mean motion has fallen below zero.
    # 3 - Perturbed eccentricity is outside the range 0 ≤ e ≤ 1.
    # 4 - Length of the orbit’s semi-latus rectum has fallen below zero.
    # 5 - (No longer used.)
    # 6 - Orbit has decayed: the computed position is underground. (The position is
    # still returned, in case the vector is helpful to software that might be searching for the moment of re-entry.)
    def track_satellite(self, time_start, time_end, time_step):
        satellite_data = []
        curr_time = time_start

        while curr_time <= time_end:
            # move all the satellites
            for satellite in self.satellites:
                try:
                    # store curr satellite pos and velocity
                    pos, vel = satellite.cal_pos_vel(curr_time)

                    # Convert pos to lat-lon format
                    lon, lat, alt = ecef2lla_ver2(pos[0], pos[1], pos[2])

                    satellite_data.append(
                        (satellite.get_name(), curr_time, lon, lat, alt, vel[0], vel[1], vel[2]))
                except RuntimeError as e:
                    print(f"Error propagating satellite {satellite.get_name()}: {str(e)}")
                    # Continue with the next satellite
                    continue

                # increment time
                curr_time += time_step
        return satellite_data

    # temp method
    def save_to_file(self, satellite_data, out_file_path: str):
        with open(out_file_path, 'w') as file:
            file.write("Satellite Name\tTime\tLongitude\tLatitude\tAltitude\tV(x) [km/s]\tV(y) [km/s]\tV(z) [km/s]\n")
            for data in satellite_data:
                file.write(
                    f"{data[0]}\t{data[1]}\t{data[2]:.6f}\t{data[3]:.6f}\t{data[4]:.6f}\t{data[5]:.6f}\t{data[6]:.6f}\t{data[7]:.6f}\n")


# FILTERING satellite positions according to user coordinates
def lie_inside_rec(lat, lon, rec_coord):
    lat_max = max(coord[0] for coord in rec_coord)
    lon_max = max(coord[1] for coord in rec_coord)

    lat_min = min(coord[0] for coord in rec_coord)
    lon_min = min(coord[1] for coord in rec_coord)

    check = (lat_min <= lat <= lat_max) and (lon_min <= lon <= lon_max)
    return check


def filter_sat_to_rect(satellite_data, user_coord):
    filtered_sat_data = []

    for data in satellite_data:
        sat_name, time, lon, lat, alt, vx, vy, vz = data
        if lie_inside_rec(lat, lon, user_coord):
            filtered_sat_data.append(data)
    return filtered_sat_data


# USER input
def get_coordinates_from_user():
    print(f'enter coordinates for the rectangle:')

    # test coordinates
    coordinates = [
        (16.66673, 103.58196),
        (69.74973, -120.64459),
        (-21.09096, -119.71009),
        (-31.32309, -147.79778)
    ]

    print(f'coordinates = ')
    for coord in coordinates:
        print(f"Latitude: {coord[0]}, Longitude: {coord[1]}")

    return coordinates


# FILE PATHS
sats_input_tle = "./Data/30sats.txt"
sats_output_tle = "./Data/30sats_output_master.txt"
##

# Main
if __name__ == '__main__':
    start_time = datetime.now()  # starting from current date
    end_time = start_time + timedelta(days=1)  # 1 day data
    time_step_ = timedelta(minutes=1)  # 1 minute increment

    # Tracker Start
    sat_tracker = SatelliteTracker(sats_input_tle)

    # verbose_debug:
    # sat_tracker.show_satellite_names()

    # find pos and vel through the day
    sat_data = sat_tracker.track_satellite(start_time, end_time, time_step_)

    # filter the data according to the user coordinates
    user_lat_long = get_coordinates_from_user()
    filter_sat_data = filter_sat_to_rect(sat_data, user_lat_long)

    # Save to file : DEBUG
    sat_tracker.save_to_file(filter_sat_data, sats_output_tle)

    print('satellites which lie in the rectangle: ')
    for sat in filter_sat_data:
        print(f'sat : {sat[0]} on {sat[1]}')
