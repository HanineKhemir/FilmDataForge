#!/usr/bin/env python3
"""
Scraper Letterboxd → Kafka
Lance depuis ton Mac : python3 letterboxd_kafka.py
"""

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json, time, random, sys

# Port 9092 mappé depuis Docker vers ton Mac
BROKERS = "localhost:9092"
TOPIC   = "movie-rating"

FILMS = [
    "inception",
    "the-dark-knight",
    "parasite-2019",
    "the-godfather",
    "oppenheimer",
    "past-lives",
    "everything-everywhere-all-at-once",
    "poor-things-2023",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def build_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

def scrape_film(slug, producer, max_pages=4):
    print(f"\n[Letterboxd] Scraping: {slug}")
    sent = 0

    for page in range(1, max_pages + 1):
        try:
            url = (f"https://letterboxd.com/film/{slug}"
                   f"/members/rated/0.5-5/page/{page}/")

            r = requests.get(url, headers=HEADERS, timeout=10)

            if r.status_code == 403:
                print(f"  Bloqué (403) page {page} — skip")
                break
            if r.status_code == 404:
                print(f"  Film non trouvé — skip")
                break

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("tr")

            if not rows:
                break

            for row in rows:
                user_el   = row.select_one("a[href]")
                rating_el = row.select_one("[class*=rated-]")

                if not user_el or not rating_el:
                    continue

                href = user_el.get("href", "")
                username = href.strip("/").split("/")[0]
                if not username or "/" in username:
                    continue

                classes    = rating_el.get("class", [])
                rating_raw = next(
                    (int(c.replace("rated-", ""))
                     for c in classes if c.startswith("rated-")),
                    None
                )
                if not rating_raw:
                    continue

                msg = {
                    "userId":    username,
                    "movieId":   slug,
                    "rating":    round(rating_raw / 2.0, 1),
                    "ratingRaw": rating_raw,
                    "source":    "letterboxd",
                    "timestamp": int(time.time() * 1000),
                }

                producer.send(TOPIC, key=slug, value=msg)
                sent += 1
                time.sleep(0.05)

            print(f"  Page {page} : {sent} ratings envoyés")
            time.sleep(random.uniform(0.5, 1.5))

        except requests.exceptions.RequestException as e:
            print(f"  Erreur réseau: {e}")
            break

    producer.flush()
    print(f"  Total pour {slug} : {sent}")
    return sent

def main():
    print("=== Letterboxd → Kafka Producer ===")
    print(f"Broker : {BROKERS}")
    print(f"Topic  : {TOPIC}")

    try:
        producer = build_producer()
        print("Connecté à Kafka ✓\n")
    except Exception as e:
        print(f"Impossible de se connecter à Kafka : {e}")
        sys.exit(1)

    try:
        cycle = 0
        while True:
            cycle += 1
            print(f"\n{'='*40}")
            print(f"Cycle {cycle}")
            print(f"{'='*40}")

            films = FILMS.copy()
            random.shuffle(films)
            total = 0

            for film in films:
                total += scrape_film(film, producer)
                time.sleep(random.uniform(2, 4))

            print(f"\nCycle {cycle} terminé — {total} ratings envoyés")
            print("Prochain cycle dans 60s...")
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nArrêt.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()