## Project Structure :

- **main.py** : Initializes the process, reading satellite data from file, tracks them for 24 hours with 1min interval,
  filters them according to user input
- **SatelliteInfo.py** : Contains `SatelliteInfo` class responsible for calculating satellite positions and velocity
- **SatelliteTracker.py** Contains the `SatelliteTracker` class that manages Spark Sessions, reads satellite data,
  tracks satellite in chunks, filters data and save results

## Optimizations

### Why Spark?

Apache Spark (`PySpark`) is used for its ability to process large datasets efficiently with distributed computing.
Its In-memory processing capabilities significantly speeds up the processing tasks. Making it suitable for handling high
volume {24 hr Satellite data} data.

### Important Methods and Functions

- **get_coord_from_user()** : Prompts user to input the geographical coord of the filtering rectangle
- **clear_output_directory(path)** : Clears output directory before writing new data
- **SatelliteInfo.cal_pos_vel(curr_time)** : Calculates position and velocity of the satellite at any given time
- **SatelliteTracker.track_satellite_chunk(chunk, time_start, time_end, time_step)**: Tracks Satellites in chunks to
  manage & save memory and processing more efficiently
- **SatelliteTracker.filter_sat_to_rect(df, user-coord)** : Filters the satellite data based on user-define lat & long.
- **udf_cal_pos_vel** : User Defined Function (UDF, from spark) ; to calculate satellite positions and velocities using
  spark
- **udf_ecef2lla_ver2** : udf to convert ecef coord to lat, long, altitude

## Running the project

1. Install python and pyspark
    ```bash 
    pip install pyspark
    ```
2. Place TLE file into `./Data/` directory
3. Run project
```bash
python3 main.py 
```

4. Access Spark UI at `http://localhost:4040` to monitor SparkJob's Progress

## Personal Notes

+ Need to efficient data processing is still there.
+ Configurations and optimisations ensure that the system resources are utilized effectively.
