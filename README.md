# Modeling Cross-Border Mobility using SoMe
"Modeling Cross-Border Mobility via Geo-Tagged Twitter in the Greater Region of Luxembourg"

Model development (python 3.6.7):
- 1 Data Acquisition & Preprocessing:
  - Acquisition: Collecting data from Twitter API using Python Tweepy
  - Preprocessing: Detecting and exluding likely bots, extracting geo-tagged rows, parsing data into Pandas DataFrame, renaming columns & producing hashed pseudo ids
- Reitit: GPS-pisteiden, risteys- ja korkeustietojen liittäminen reittisegmentteihin
- pyorailyverkosto: Tieverkon muokkaus
