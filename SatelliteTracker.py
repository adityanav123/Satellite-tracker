import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from sgp4.api import jday, Satrec, WGS84

from SatelliteInfo import SatelliteInfo, ecef2lla_ver2


class SatelliteTracker:
    def __init__(self, file_path):
        self.satellites = self.parse_file(file_path)

        # Issue: Too much Resource consumption
        # Fix: assigning resources according to system
        total_mem = psutil.virtual_memory().total // (1024 * 1024)  # MB
        available_mem = psutil.virtual_memory().available // (1024 * 1024)  # MB
        num_cores = psutil.cpu_count()

        executor_mem = f'{min(available_mem // 2, 8192)}m'  # Cap at 2gb
        driver_mem = f'{min(available_mem // 2, 8192)}m'
        spark_ui_port = f'4040'
        parallel_core_cnt = f'{num_cores * 2}'
        extra_java_opts = f'-XX:+UseG1GC'

        # Add (For Debug) : Spark UI
        self.spark = (SparkSession.builder.appName("SatelliteTracker")
                      .config("spark.executor.memory", executor_mem)
                      .config("spark.driver.memory", driver_mem)
                      .config("spark.sql.shuffle.partitions", parallel_core_cnt)
                      .config("spark.default.parallelism", parallel_core_cnt)
                      .config("spark.ui.port", spark_ui_port)
                      .config("spark.executor.extraJavaOptions", extra_java_opts)
                      .config("spark.driver.extraJavaOptions", extra_java_opts)
                      .config("spark.task.cpus", "1")
                      .config("spark.task.maxFailures", "4")
                      .getOrCreate())

    def __del__(self):
        self.shutdown()

    def parse_file(self, path) -> [SatelliteInfo]:
        satellites = []
        with open(path, 'r') as file:
            lines = file.readlines()
            for idx in range(0, len(lines), 3):
                satellite_name = lines[idx].strip()
                line_1 = lines[idx + 1].strip()
                line_2 = lines[idx + 2].strip()
                satellites.append(SatelliteInfo(satellite_name, line_1, line_2))
            return satellites

    def track_satellite(self, time_start, time_end, time_step):
        satellite_data = []
        curr_time = time_start
        while curr_time <= time_end:
            satellite_data += [(sat.name, sat.line_1, sat.line_2, curr_time) for sat in self.satellites]
            curr_time += time_step
        return satellite_data

    # track satellite in chunks
    def track_satellite_chunk(self, chunk, time_start, time_end, time_step):
        satellite_data = []
        curr_time = time_start
        while curr_time <= time_end:
            satellite_data += [(sat.name, sat.line_1, sat.line_2, curr_time) for sat in chunk]
            curr_time += time_step
        return satellite_data

    def filter_sat_to_rect(self, df, user_coord):
        lat_min = min(coord[0] for coord in user_coord)
        lat_max = max(coord[0] for coord in user_coord)
        lon_min = min(coord[1] for coord in user_coord)
        lon_max = max(coord[1] for coord in user_coord)

        # check if satellite coord lie in the boundary.
        return df.filter(
            (df["lat"] >= lat_min) & (df["lat"] <= lat_max) &
            (df["lon"] >= lon_min) & (df["lon"] <= lon_max)
        )

    def save_to_file(self, sat_df, out_file_path):
        sat_df.write.mode("append").csv(out_file_path, sep='\t', header=True)

    def shutdown(self):
        if self.spark:
            self.spark.stop()


def cal_pos_vel(sat_line1, sat_line2, curr_time):
    satrec = Satrec.twoline2rv(sat_line1, sat_line2, WGS84)
    jd, fr = jday(curr_time.year, curr_time.month, curr_time.day, curr_time.hour, curr_time.minute, curr_time.second)
    err, pos, vel = satrec.sgp4(jd, fr)
    if err != 0:
        return [None, None]
    return [pos, vel]


# User Defined Functions
udf_cal_pos_vel = udf(lambda sat_line1, sat_line2, curr_time: cal_pos_vel(sat_line1, sat_line2, curr_time),
                      ArrayType(ArrayType(DoubleType())))
udf_ecef2lla = udf(lambda pos: ecef2lla_ver2(pos), ArrayType(DoubleType()))
