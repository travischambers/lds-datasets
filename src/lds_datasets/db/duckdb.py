"""Module for DuckDB database operations."""
import duckdb

stakes = duckdb.read_json("data/stakes_2024_02_04.json")
stakes_added = duckdb.read_json("data/daily/*/stakes_added.json")
stakes_removed = duckdb.read_json("data/daily/*/stakes_removed.json")

wards = duckdb.read_json("data/wards_2024_02_04.json")
wards_added = duckdb.read_json("data/daily/*/wards_added.json")
wards_removed = duckdb.read_json("data/daily/*/wards_removed.json")

# stakes per state in USA
duckdb.sql("SELECT address.state, COUNT(*) FROM stakes WHERE address.countryCode3 = 'USA' GROUP BY address.state ORDER BY address.state")
# wards per state in the USA
duckdb.sql("SELECT address.state, COUNT(*) FROM wards WHERE address.countryCode3 = 'USA' GROUP BY address.state ORDER BY address.state")
# wards per stake per state in the USA
duckdb.sql("""SELECT s.state,
       s.stake_count,
       w.ward_count,
       w.ward_count / s.stake_count AS wards_per_stake
FROM
  (SELECT address.state,
          COUNT(*) AS stake_count
   FROM stakes
   WHERE address.countryCode3 = 'USA'
   GROUP BY address.state) s
JOIN
  (SELECT address.state,
          COUNT(*) AS ward_count
   FROM wards
   WHERE address.countryCode3 = 'USA'
   GROUP BY address.state) w ON s.state = w.state
ORDER BY s.state;""")

# wards added per state in the USA
wards_added_per_state = duckdb.sql("SELECT address.state, COUNT(*) as count FROM wards_added WHERE address.countryCode3 = 'USA' GROUP BY address.state ORDER BY count DESC")
# wards removed per state in the USA
wards_removed_per_state = duckdb.sql("SELECT address.state, COUNT(*) as count FROM wards_removed WHERE address.countryCode3 = 'USA' GROUP BY address.state ORDER BY count DESC")

# stakes per country
duckdb.sql("SELECT address.country, COUNT(*) FROM stakes GROUP BY address.country ORDER BY COUNT(*) DESC")
# wards per country
duckdb.sql("SELECT address.country, COUNT(*) FROM wards GROUP BY address.country ORDER BY COUNT(*) DESC")
# wards per stake per country
duckdb.sql("""
           SELECT s.country,
       s.stake_count,
       w.ward_count,
       w.ward_count / s.stake_count AS wards_per_stake
FROM
  (SELECT address.country,
          COUNT(*) AS stake_count
   FROM stakes
   GROUP BY address.country) s
JOIN
  (SELECT address.country,
          COUNT(*) AS ward_count
   FROM wards
   GROUP BY address.country) w ON s.country = w.country
ORDER BY s.stake_count DESC;
""")
