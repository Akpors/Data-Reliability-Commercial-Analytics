# LinkedIn post draft

The main post is about 1,260 characters, hashtags included. Replace `[VIDEO LINK]` and `[REPO LINK]` before posting.

## Main post

Most analytics failures don't start in the dashboard. They start much earlier.

So I built a small data platform (Airflow 3, dbt, Postgres, Streamlit) and broke it on purpose. I injected eight kinds of failure: duplicates, missing dates, broken keys, schema changes, negative revenue, stale data, volume spikes and a flaky API.

What I learned:

1. Let data arrival drive the pipeline. Files trigger the work through Airflow Assets, not a clock. Keep a fallback check for the day nothing arrives.

2. Check contracts at the edge. A renamed column is caught at ingestion and the file is held. You can fix the source, or accept the change and replay the file. Nothing is lost.

3. Detectors must be generic. None of mine knows what I injected. They watch tests, freshness, row counts and run times, the signals a real failure would leave.

4. Treat failures as incidents with a lifecycle. Each one shows how far downstream it reaches, and it closes on its own once the data is fixed. Blocking tests keep bad data out, so the dashboard is a day behind, never wrong.

One command checks all eight: inject, detect, fix, resolve.

Demo: [VIDEO LINK]
Code (runs with one command): [REPO LINK]

#DataEngineering #DataQuality #ApacheAirflow #dbt #AnalyticsEngineering

## Shorter alternative

Most analytics failures don't start in the dashboard. They start much earlier.

I built a mini data platform (Airflow 3, dbt, Postgres, Streamlit) and broke it eight ways: duplicates, schema changes, stale data, volume spikes and more. Every failure is caught by a generic check, shown with its blast radius, and resolved automatically once the data is fixed.

The biggest lesson: blocking bad data is better than reporting wrong numbers. A dashboard that's a day behind, with an incident explaining why, beats one that's confidently wrong.

Demo: [VIDEO LINK] | Code: [REPO LINK]
