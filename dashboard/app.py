from flask import Flask, jsonify, render_template
import subprocess
import io
import csv

app = Flask(__name__)
CONTAINER = "hadoop-master"


def hdfs_cat(path):
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "hdfs", "dfs", "-cat", path],
        capture_output=True, text=True, timeout=20
    )
    return result.stdout.strip()


def hdfs_exists(path):
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "hdfs", "dfs", "-test", "-e", path],
        capture_output=True
    )
    return result.returncode == 0


def parse_csv(content):
    if not content:
        return []
    try:
        reader = csv.DictReader(io.StringIO(content))
        return [dict(row) for row in reader]
    except Exception:
        return []


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def status():
    return jsonify({
        "mapreduce": hdfs_exists("/movielens/output-mr/part-r-00000"),
        "spark_batch": hdfs_exists("/movielens/output-spark/top10"),
        "streaming": hdfs_exists("/movielens/output-streaming/raw-stream"),
    })


@app.route("/api/mapreduce")
def mapreduce():
    content = hdfs_cat("/movielens/output-mr/part-r-00000")
    results = []
    for line in content.split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            try:
                results.append({
                    "movieId": parts[0],
                    "avgRating": round(float(parts[1]), 2),
                    "count": int(parts[2]),
                })
            except (ValueError, IndexError):
                continue
    results.sort(key=lambda x: x["avgRating"], reverse=True)
    return jsonify(results[:20])


@app.route("/api/top10")
def top10():
    content = hdfs_cat("/movielens/output-spark/top10/*.csv")
    return jsonify(parse_csv(content))


@app.route("/api/genre-stats")
def genre_stats():
    content = hdfs_cat("/movielens/output-spark/genre-stats/*.csv")
    rows = parse_csv(content)
    rows.sort(key=lambda x: float(x.get("avgRating", 0)), reverse=True)
    return jsonify(rows)


@app.route("/api/rating-distribution")
def rating_distribution():
    content = hdfs_cat("/movielens/output-spark/rating-distribution/*.csv")
    rows = parse_csv(content)
    rows.sort(key=lambda x: float(x.get("rating", 0)))
    return jsonify(rows)


@app.route("/api/streaming")
def streaming():
    content = hdfs_cat("/movielens/output-streaming/raw-stream/*.csv")
    rows = parse_csv(content)
    return jsonify(rows[-100:] if len(rows) > 100 else rows)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
