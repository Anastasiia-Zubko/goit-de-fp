USE olympic_dataset;

CREATE TABLE athlete_enriched_agg_zubko (
    sport VARCHAR(255),
    medal VARCHAR(50),
    sex VARCHAR(10),
    country_noc VARCHAR(10),
    avg_height FLOAT,
    avg_weight FLOAT,
    timestamp TIMESTAMP
);