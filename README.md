# Modeling Cross-Border Mobility Using Social Media (Twitter)
Samuli Massinen's MSc Thesis "Modeling Cross-Border Mobility Using Geotagged Twitter in the Greater Region of Luxembourg"

Model development (Python 3.6.7):
- 1 Data Acquisition & Preprocessing:
  - 1.1 Acquisition: Collecting data from Twitter API using Tweepy
  - 1.2 Preprocessing: Detecting and exluding likely bots, extracting geotagged rows, parsing data into Pandas, renaming columns & producing hashed pseudo ids
- 2 Analysis
  - 2.1 Home Detection: User home country detection, classifying home regions as well as identifying and assigning dominance areas
  - 2.2 Cross-Border Mobility Patterns
    - 2.2.1 Cross-Border Movements: Creating cross-border movements (LineStrings) from Twitter point data
    - 2.2.2 Activity Nodes: Calculating median centroids (users and countries) for the Greater Region of Luxembourg, identifying daily life spaces of people
    - 2.2.3 Mover Type Identification: Classifying daily cross-border movers & infrequent border crossers
    - 2.2.4 Temporal Variation: Movement activity comparison for weekdays
- 3 Accessories
  - Accessory scripts: Calculating tweet counts per user, user geotag statistics
