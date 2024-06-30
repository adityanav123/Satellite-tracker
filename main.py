# Logging
import datetime
import logging
import os
import shutil
import timeit

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from SatelliteTracker import SatelliteTracker, udf_cal_pos_vel, udf_ecef2lla

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_coord_from_user():
    print(f'enter coordinates of rectangle: ')
    coord = [
        (16.66673, 103.58196),
        (69.74973, -120.64459),
        (-21.09096, -119.71009),
        (-31.32309, -147.79778)
    ]

    return coord


def clear_output_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def main():
    sats_input = './Data/50sats.txt'
    sats_output = './Data/50sats_output.txt'

    start_time = datetime.datetime.now()
    end_time = datetime.timedelta(days=1) + start_time
    time_step = datetime.timedelta(minutes=1)

    try:
        clear_output_directory(sats_output)
        print(f'Spark UI should be enabled at : http://localhost:4040')
        sat_tracker = SatelliteTracker(sats_input)
        user_lat_long = get_coord_from_user()

        # Process the data in chunks.
        chunk_size = 250
        total_filtered_sat = []
        for i in range(0, len(sat_tracker.satellites), chunk_size):
            chunk = sat_tracker.satellites[i:i + chunk_size]
            # satellite_data = sat_tracker.track_satellite(start_time, end_time, time_step)
            satellite_data = sat_tracker.track_satellite_chunk(chunk, start_time, end_time, time_step)

            schema = StructType([
                StructField("name", StringType(), True),
                StructField("line1", StringType(), True),
                StructField("line2", StringType(), True),
                StructField("curr_time", TimestampType(), True)
            ])

            sat_df = sat_tracker.spark.createDataFrame(satellite_data, schema=schema)

            # repartition for improving performance
            sat_df = sat_df.repartition(200)

            sat_df = sat_df.withColumn("pos_vel", udf_cal_pos_vel("line1", "line2", "curr_time"))
            sat_df = sat_df.withColumn("pos", col("pos_vel")[0])
            sat_df = sat_df.withColumn("vel", col("pos_vel")[1])
            sat_df = sat_df.withColumn("lla", udf_ecef2lla("pos"))
            sat_df = sat_df.cache()

            # Bug Fix: creating columns from `lla`
            sat_df = sat_df.withColumn("lon", col("lla").getItem(0))
            sat_df = sat_df.withColumn("lat", col("lla").getItem(1))
            sat_df = sat_df.withColumn("alt", col("lla").getItem(2))
            sat_df = sat_df.withColumn("vel_x", col("vel").getItem(0))
            sat_df = sat_df.withColumn("vel_y", col("vel").getItem(1))
            sat_df = sat_df.withColumn("vel_z", col("vel").getItem(2))

            sat_df = sat_df.select("name", "curr_time", "lon", "lat", "alt", "vel_x", "vel_y", "vel_z")

            # filter from user input
            filtered_sat_df = sat_tracker.filter_sat_to_rect(sat_df, user_lat_long)
            total_filtered_sat.append(filtered_sat_df)

            # Stage: Saving to file and printing on CL Output (Adds 1 stage to spark execution)
            # sat_tracker.save_to_file(filtered_sat_df, sats_output)
            filtered_sat_df.write.mode("append").csv(sats_output, sep='\t', header=True)

        # Merging all the filtered data frames
        if total_filtered_sat:
            final_filtered_sats = total_filtered_sat[0]
            for df in total_filtered_sat[1:]:
                final_filtered_sats = final_filtered_sats.union(df)

            logger.info('Satellites which lie in the rectangle: ')
            final_filtered_sats.show()

    except Exception as e:
        logger.error(f'An Error occurred: {str(e)}')


if __name__ == '__main__':
    execution_time = timeit.timeit(main, number=1)
    logger.info(f'Execution time (time-it) : {execution_time} sec')
