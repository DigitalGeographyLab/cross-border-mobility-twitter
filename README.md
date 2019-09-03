# Modeling Cross-Border Mobility Using SoMe
Samuli Massinen's MSc Thesis "Modeling Cross-Border Mobility via Geo-Tagged Twitter in the Greater Region of Luxembourg"

Model development (Python 3.6.7):
- 1 Data Acquisition & Preprocessing:
  - 1.1 Acquisition: Collecting data from Twitter API using Tweepy
  - 1.2 Preprocessing: Detecting and exluding likely bots, extracting geo-tagged rows, parsing data into Pandas, renaming columns & producing hashed pseudo ids
- 2 Analysis
  - 2.1 Home Detection: User home country detection, identifying and assigning home region
  - 2.2 Cross-Border Mobility Patterns
    - 2.2.1 Cross-Border Movements: Creating cross-border movements (LineStrings) from Twitter point data
    - 2.2.2 Activity Nodes: Calculating median activity centroids (users and countries) for the Greater Region of Luxembourg
    - 2.2.3 Mover Type Identification: Classifying cross-border commuters & infrequent border crossers
    - 2.2.4 Temporal Variation: Movement activity comparison for weekdays
- 3 Accessories
  - Accessory scripts: Calculating tweet counts per user, user geo-tag statistics
