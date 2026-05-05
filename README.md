# Big-Data-MovieLens-

A Java big-data project that processes the [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/) using Hadoop MapReduce, Spark batch analytics, and Spark Streaming + Kafka.

## Project Structure

This project uses Apache Hadoop, Spark, Kafka, and Maven.

- **`RatingMapper`**: Reads raw `ratings.dat` data and emits `(Movie ID, Rating)`.
- **`RatingReducer`**: Aggregates all ratings for each Movie ID, calculating the average rating and the total count.
- **`MovieRatingAverage`**: The MapReduce driver class that configures and launches the Hadoop job.
- **`MovieLensAnalysis`**: Spark batch analysis (top 10 movies, genre stats, ratings distribution, full movie stats).
- **`RatingProducer`**: Kafka producer that streams `ratings.dat` to the `movie-rating` topic.
- **`LetterboxdProducer`**: TMDB-backed Kafka producer that publishes popular movie ratings from the TMDB API.
- **`RatingStreamProcessor`**: Spark Structured Streaming job that reads Kafka and computes live averages per movie.

## Prerequisites

- Java 8
- Apache Maven
- A running Hadoop + Spark cluster (or a Dockerized Hadoop container like `hadoop-master`)
- Kafka broker running (for streaming)

## Data Format

**Input (`ratings.dat`) format:**
```
userId::movieId::rating::timestamp
```

**MapReduce output format:**
```
movieId    average_rating    number_of_ratings
```

**Spark batch outputs (CSV):**
- `movie-stats` (all movies)
- `top10` (top 10 movies, min 100 votes)
- `genre-stats` (avg rating by genre)
- `rating-distribution` (counts by rating)

## How to Build

Compile and package the project into a single "fat JAR" with all dependencies using Maven:

```bash
mvn clean package
```

The compiled JAR will be created in the `target/` directory:
`target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar`

## How to Run (MapReduce)

1. **Upload the Input Data to HDFS:**
   ```bash
   hdfs dfs -mkdir -p /movielens
   hdfs dfs -put ratings.dat /movielens/
   ```

2. **Execute the Job:**
   Make sure you clear the output directory first (Hadoop requires the output folder to not exist before running):
   ```bash
   hdfs dfs -rm -r /movielens/output-mr
   hadoop jar target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar movielens.MovieRatingAverage /movielens/ratings.dat /movielens/output-mr
   ```

3. **View the Results:**
   ```bash
   hdfs dfs -cat /movielens/output-mr/part-r-00000
   ```

## How to Run (Spark Batch)

1. **Upload input files to HDFS:**
   ```bash
   hdfs dfs -mkdir -p /movielens
   hdfs dfs -put ratings.dat /movielens/
   hdfs dfs -put movies.dat /movielens/
   ```

2. **Run Spark batch analysis:**
   ```bash
   spark-submit \
     --class movielens.spark.MovieLensAnalysis \
     --master local[*] \
     target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
     /movielens/ratings.dat \
     /movielens/movies.dat \
     /movielens/output-spark
   ```

3. **View results (CSV files use random part names):**
   ```bash
   hdfs dfs -cat /movielens/output-spark/top10/*.csv
   hdfs dfs -cat /movielens/output-spark/genre-stats/*.csv
   hdfs dfs -cat /movielens/output-spark/rating-distribution/*.csv
   hdfs dfs -cat /movielens/output-spark/movie-stats/*.csv
   ```

## How to Run (Kafka + Spark Streaming)

1. **Create the Kafka topic:**
   ```bash
   kafka-topics.sh --create \
     --topic movie-rating \
     --replication-factor 1 \
     --partitions 1 \
     --bootstrap-server localhost:9092
   ```

2. **Start the Spark streaming job:**
   ```bash
   spark-submit \
     --class movielens.kafka.RatingStreamProcessor \
     --master local[2] \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
     /movielens/output-streaming
   ```

3. **Start the Kafka producer:**
   ```bash
   java -cp target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
     movielens.kafka.RatingProducer /movielens/ratings.dat
   ```

4. **TMDB producer (no scraping):**
    Export your TMDB bearer token and run the producer to publish popular movies.
   Do not commit the token to git.
    ```bash
    export TMDB_TOKEN="<your_tmdb_bearer_token>"
    java -cp target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
       movielens.kafka.LetterboxdProducer
    ```

    Optional arguments:
    - `--loop` → run continuously
    - `<pages>` → number of TMDB pages to fetch per cycle (default: 1)

4. **Streaming output:**
   The console will display live `movieId`, `avgRating`, and `totalVotes` updates.