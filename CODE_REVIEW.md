# Code Inconsistency Review

## ✅ PASSED CHECKS

### 1. **Kafka Topics — CONSISTENT**
All three classes correctly use separate topics:
- `movielens.mock.RatingProducer` → publishes to **`movielens-ratings`** ✓
- `movielens.streamingkafka.LetterboxdProducer` → publishes to **`letterboxd-ratings`** ✓
- `movielens.streamingkafka.RatingStreamProcessor` → subscribes to BOTH topics ✓

### 2. **Package Names — CONSISTENT**
- `movielens.mock` — for batch/offline data producer
- `movielens.streamingkafka` — for real-time streaming components
- Clear semantic distinction ✓

### 3. **JSON Schema for Letterboxd — CONSISTENT**
Both producer and processor agree on the schema:
```json
{
  "userId": "string",
  "movieId": "string",
  "rating": double,
  "ratingRaw": int,
  "source": "letterboxd",
  "timestamp": long
}
```

✓ LetterboxdProducer sends all 6 fields
✓ RatingStreamProcessor schema includes all 6 fields

### 4. **Broker Configuration — CONSISTENT**
All three classes use: `localhost:9092` ✓

### 5. **MovieLens Format — CONSISTENT**
RatingProducer sends: `userId::movieId::rating::timestamp`
RatingStreamProcessor parses: splits on `::`  with correct item indices ✓

### 6. **HBase Configuration — CONSISTENT**
- Zookeeper: `localhost`
- Port: `2181`
- Table: `movie_stats`
- Row key format: `movieId_source` (e.g., `inception_letterboxd`)
- Column families: `ratings`, `info` ✓

### 7. **Output Mode & Triggers — CONSISTENT**
| Output | Mode | Trigger |
|--------|------|---------|
| Console (aggregated) | `complete` | 5s |
| Source stats | `complete` | 10s |
| HDFS archive | `append` | 10s |
| HBase | `update` (foreachBatch) | 10s |

All consistent with streaming semantics ✓

### 8. **Maven Dependencies — CONSISTENT**
- Hadoop 3.3.6 (provided scope for cluster runtime)
- Spark 3.5.0 (provided scope)
- Kafka 3.6.1 (bundled in fat JAR)
- JSoup 1.17.2 (bundled for scraping)
- HBase 2.5.8 (bundled)
- spark-sql-kafka integration (bundled)

All aligned with Java 8 target ✓

### 9. **README Documentation — CONSISTENT**
Instructions mention:
- `movielens.mock.RatingProducer` (updated) ✓
- `movielens.streamingkafka.LetterboxdProducer` (updated) ✓
- `movielens.streamingkafka.RatingStreamProcessor` (updated) ✓

---

## ⚠️ ISSUES FOUND

### Issue 1: README References Old Topic Name
**Location:** `README.md` line ~94
**Current Text:**
```bash
kafka-topics.sh --create \
  --topic movie-rating \
  --replication-factor 1 \
  --partitions 1 \
  --bootstrap-server localhost:9092
```

**Problem:** References old `movie-rating` topic (now we have `movielens-ratings` and `letterboxd-ratings`)

**Severity:** ⚠️ MEDIUM — Documentation misleading; users following README won't have correct topics

**Fix:** Update README to show both topics

---

### Issue 2: RatingProducer Missing Topic Creation
**Location:** `movielens/mock/RatingProducer.java`
**Current State:** No `ensureTopicExists()` call (unlike LetterboxdProducer)

**Problem:** 
- LetterboxdProducer creates its topic on startup
- RatingProducer relies on topic pre-existing
- **Asymmetrical behavior** — one producer auto-creates, the other doesn't

**Severity:** ⚠️ MEDIUM — Could cause failure if `movielens-ratings` topic doesn't exist

**Fix:** Add topic creation to RatingProducer

---

### Issue 3: RatingStreamProcessor Uses Deprecated Output Mode
**Location:** `RatingStreamProcessor.java` line 162
**Current Code:**
```java
.outputMode("update")
.foreachBatch((batch, batchId) -> {
    writeToHBase(batch);
})
```

**Problem:** 
- `foreachBatch()` requires `outputMode("append")` or `outputMode("complete")`
- `outputMode("update")` is only for sink formats that natively support it (not `foreachBatch`)
- This is **technically incorrect** but may work due to internal Spark behavior

**Severity:** ⚠️ HIGH — May fail or behave unexpectedly in production

**Fix:** Change to `outputMode("append")`

---

### Issue 4: Missing Error Handling in HBase Write
**Location:** `RatingStreamProcessor.java` lines 195-225
**Current State:**
```java
String movieId = String.valueOf(row.getAs("movieId"));
if (movieId == null || movieId.equals("null")) continue;
```

**Problem:**
- Checks for `"null"` string, but `String.valueOf()` can produce `null` reference first
- Logic: `String.valueOf(null)` → `"null"` (string), so the check catches it
- **Confusing but technically works**, but poor defensive programming
- No null check for `source` field before using in row key

**Severity:** ⚠️ LOW-MEDIUM — Unlikely but could cause NullPointerException

**Fix:** Add explicit null checks before `String.valueOf()`

---

### Issue 5: LetterboxdProducer HTML Parsing May Fail Silently
**Location:** `LetterboxdProducer.java` lines 150-156
**Current State:**
```java
Elements rows = doc.select("tr.table-film-detail");

if (rows.isEmpty()) {
    // Essayer un autre sélecteur
    rows = doc.select("tr");
}

if (rows.isEmpty()) break; // plus de pages
```

**Problem:**
- Selector `"tr.table-film-detail"` may not match actual Letterboxd HTML
- Fallback `"tr"` will match **ALL** table rows, not just ratings
- Could extract garbage data without error
- Silent failure: if no rows found, loop just stops without logging

**Severity:** ⚠️ MEDIUM — Data quality issue; no validation of extracted data

**Fix:** Add logging; validate extracted fields

---

### Issue 6: RatingProducer No Linger/Batch Config
**Location:** `RatingProducer.java` lines 25-26
**Comparison:** LetterboxdProducer has:
```java
props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
```

**Problem:**
- RatingProducer sends 10ms apart with `Thread.sleep(10)`
- LetterboxdProducer uses batching with 100ms linger
- **Inconsistent performance tuning**
- RatingProducer will have higher latency/overhead (unbatched messages)

**Severity:** ⚠️ LOW — Works, but suboptimal batching

**Fix:** Add `LINGER_MS_CONFIG` to RatingProducer for consistency

---

### Issue 7: Missing Admin Topic Creation in RatingProducer
**Location:** `RatingProducer.java` vs `LetterboxdProducer.java`
**Current State:** Only LetterboxdProducer calls `ensureTopicExists()`

**Problem:**
- **Asymmetrical behavior**: two producers, only one creates topics
- If user runs RatingProducer before topic exists → fails silently (Kafka will auto-create with default settings)
- **Best practice**: all producers should ensure their topics exist

**Severity:** ⚠️ MEDIUM — Operational inconsistency

**Fix:** Add topic creation to RatingProducer

---

### Issue 8: RatingStreamProcessor JSON Schema Includes Optional Fields
**Location:** `RatingStreamProcessor.java` lines 72-78
**Current Schema:**
```java
.add("userId",    DataTypes.StringType)
.add("movieId",   DataTypes.StringType)
.add("rating",    DataTypes.DoubleType)
.add("ratingRaw", DataTypes.DoubleType)      // ← may not be sent?
.add("source",    DataTypes.StringType)
.add("timestamp", DataTypes.LongType);
```

**Problem:**
- Processor extracts all 6 fields, but selects only: userId, movieId, rating, source
- Fields `ratingRaw` and `timestamp` are **unused** after parsing
- Creates **dead code** in schema definition

**Severity:** ⚠️ LOW — Works, but schema bloat

**Fix:** Remove unused fields from schema

---

## 🔧 RECOMMENDATIONS

### Priority 1 (Fix Now):
1. ✅ **Fix outputMode for HBase sink**: Change `"update"` → `"append"`
2. ✅ **Add topic creation to RatingProducer**: Use AdminClient like LetterboxdProducer
3. ✅ **Update README**: Document both `movielens-ratings` and `letterboxd-ratings` topics

### Priority 2 (Improve):
4. ✅ **Add null checks**: Defensive programming in HBase write
5. ✅ **Add logging in LetterboxdProducer**: Log when no rows found/extracted
6. ✅ **Remove dead schema fields**: Streamline JSON schema

### Priority 3 (Optimize):
7. ✅ **Add batching config to RatingProducer**: Add `LINGER_MS_CONFIG` for consistency
8. ✅ **Validate extracted data**: Check username/rating not empty before sending

---

## Summary

| Category | Status | Count |
|----------|--------|-------|
| ✅ Passed | Consistent | 9 |
| ⚠️ Issues | Found | 8 |
| 🔴 Critical | None | 0 |
| 🟡 High | 1 | outputMode issue |
| 🟠 Medium | 4 | Topic creation, HTML parsing, error handling, readme |
| 🟢 Low | 3 | Batching, schema bloat, logging |

**Overall Assessment:** Code is functionally correct and compiles, but has **architectural inconsistencies** and **defensive programming gaps** that should be addressed before production use.
