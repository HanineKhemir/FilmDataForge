# Big-Data-MovieLens-

A Java big-data project that processes the [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/) using Hadoop MapReduce, Spark batch analytics, and Spark Streaming + Kafka.

## Project Structure

This project uses Apache Hadoop, Spark, Kafka, and Maven.

- **`RatingMapper`**: Reads raw `ratings.dat` data and emits `(Movie ID, Rating)`.
- **`RatingReducer`**: Aggregates all ratings for each Movie ID, calculating the average rating and the total count.
- **`MovieRatingAverage`**: The MapReduce driver class that configures and launches the Hadoop job.
- **`MovieLensAnalysis`**: Spark batch analysis (top 10 movies, genre stats, ratings distribution, full movie stats).
- **`RatingProducer`**: Kafka producer that streams `ratings.dat` to the `movielens-ratings` topic (mock/offline data).
- **`LetterboxdProducer`**: Real-time scraping producer (Letterboxd) that sends ratings to Kafka.
- **`RatingStreamProcessor`**: Spark Structured Streaming job that reads Kafka and computes live averages per movie.

## Prerequisites

- Java 8
- Apache Maven
- A running Hadoop + Spark cluster (or a Dockerized Hadoop container like `hadoop-master`)
- Kafka broker running (for streaming)

## Architecture

### High-level flow

1. **Batch processing (Hadoop + Spark)**
   - **MapReduce**: `RatingMapper` → `RatingReducer` → `MovieRatingAverage` (averages + counts).
   - **Spark batch**: `movielens.spark.MovieLensAnalysis` (top10, genres, distributions, full stats).
   - **Storage**: results written to **HDFS** (CSV/text outputs).

2. **Streaming processing (Kafka + Spark Structured Streaming)**
   - **Producer (MovieLens replay)**: `movielens.mock.RatingProducer` publishes `ratings.dat` to **`movielens-ratings`**.
   - **Producer (Letterboxd live)**: `movielens.streamingkafka.LetterboxdProducer` publishes to **`letterboxd-ratings`**.
   - **Streaming consumer**: `movielens.streamingkafka.RatingStreamProcessor` consumes both topics, normalizes, aggregates.
   - **Outputs**: console metrics, raw stream archived to **HDFS**, aggregates stored in **HBase**.

3. **Storage layer**
   - **HDFS**: batch outputs + streaming raw archive.
   - **HBase**: live aggregates in `movie_stats` (row key: `movieId_source`).

<img width="930" height="333" alt="image" src="https://github.com/user-attachments/assets/2b556474-b424-466b-90df-9c0d4421e68f" />


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

### 1. Create Kafka Topics

Both topics are **auto-created** by the producers, but you can create them manually:

```bash
# Topic for MovieLens batch data (mock producer)
kafka-topics.sh --create \
  --topic movielens-ratings \
  --replication-factor 1 \
  --partitions 1 \
  --bootstrap-server localhost:9092

# Topic for Letterboxd live data (scraper)
kafka-topics.sh --create \
  --topic letterboxd-ratings \
  --replication-factor 1 \
  --partitions 1 \
  --bootstrap-server localhost:9092
```

### 2. Start the Spark Streaming Job

```bash
spark-submit \
  --class movielens.streamingkafka.RatingStreamProcessor \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /movielens/output-streaming
```

### 3. Start the MovieLens Producer (Batch/Mock Data)

```bash
java -cp target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
  movielens.mock.RatingProducer /movielens/ratings.dat
```

### 4. Start the Letterboxd Producer (Real-time Scraper)

```bash
java -cp target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
  movielens.streamingkafka.LetterboxdProducer
```

**Optional arguments:**
- `--loop` → run continuously (cycle through films every 60s)
- `<pages>` → number of pages to fetch per film (default: 3)

**Example with options:**
```bash
java -cp target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \
  movielens.streamingkafka.LetterboxdProducer --loop 5
```

### 5. Monitor Output

The Spark streaming console will display:
- **console_output**: Per-movie aggregations (every 5s)
- **source_stats**: Stats grouped by source (every 10s)
- **hdfs_archive**: Raw data saved to HDFS
- **hbase_writer**: Results written to HBase `movie_stats` table

---

### Architecture

See the **Architecture** section above for the full batch + streaming flow and diagram.
