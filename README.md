# Big-Data-MovieLens-

A Hadoop MapReduce application written in Java that processes the [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/) to calculate the average rating and the total number of ratings for each individual movie.

## Project Structure

This project uses Apache Hadoop and Maven.

- **`RatingMapper`**: Reads raw `ratings.dat` data and emits `(Movie ID, Rating)`.
- **`RatingReducer`**: Aggregates all ratings for each Movie ID, calculating the average rating and the total count.
- **`MovieRatingAverage`**: The MapReduce driver class that configures and launches the Hadoop job.

## Prerequisites

- Java 8
- Apache Maven
- A running Hadoop Cluster (or a Dockerized Hadoop container like `hadoop-master`)

## Data Format

**Input (`ratings.dat`) format:**
```
userId::movieId::rating::timestamp
```

**Output format:**
```
movieId    average_rating    number_of_ratings
```

## How to Build

Compile and package the project into a single "fat JAR" with all dependencies using Maven:

```bash
mvn clean package
```

The compiled JAR will be created in the `target/` directory:
`target/movielens-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar`

## How to Run

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