"""Module for DuckDB database operations."""
import duckdb

stakes = duckdb.read_json("data/stakes_2024_02_04.json")
stakes_added = duckdb.read_json("data/daily/*/stakes_added.json")
stakes_removed = duckdb.read_json("data/daily/*/stakes_removed.json")

wards = duckdb.read_json("data/wards_2024_02_04.json")
wards_added = duckdb.read_json("data/daily/*/wards_added.json")
wards_removed = duckdb.read_json("data/daily/*/wards_removed.json")

branches_added = duckdb.read_json("data/daily/*/branches_added.json")
branches_removed = duckdb.read_json("data/daily/*/branches_removed.json")

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

# combined stakes added, removed, and net change per state in the USA
net_change_usa_stakes = duckdb.sql("""
SELECT COALESCE(a.state, r.state) AS state,
            COALESCE(a.added, 0) AS added,
            COALESCE(r.removed, 0) AS removed,
            COALESCE(a.added, 0) - COALESCE(r.removed, 0) AS net_change
FROM
    (SELECT address.state,
            COUNT(*) AS added
     FROM stakes_added
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) a
FULL OUTER JOIN
    (SELECT address.state,
            COUNT(*) AS removed
     FROM stakes_removed
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) r ON a.state = r.state
ORDER BY net_change ASC;
""")


# combined wards added, removed, and net change per state in the USA
net_change_usa_wards = duckdb.sql("""
SELECT COALESCE(a.state, r.state) AS state,
         COALESCE(a.added, 0) AS added,
         COALESCE(r.removed, 0) AS removed,
         COALESCE(a.added, 0) - COALESCE(r.removed, 0) AS net_change
FROM
    (SELECT address.state,
            COUNT(*) AS added
     FROM wards_added
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) a
FULL OUTER JOIN
    (SELECT address.state,
            COUNT(*) AS removed
     FROM wards_removed
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) r ON a.state = r.state
ORDER BY net_change ASC;
""")

net_change_usa_branches = duckdb.sql("""
SELECT a.state,
            a.added,
            r.removed,
            a.added - r.removed AS net_change
FROM
    (SELECT address.state,
            COUNT(*) AS added
     FROM branches_added
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) a
OUTER JOIN
    (SELECT address.state,
            COUNT(*) AS removed
     FROM branches_removed
     WHERE address.countryCode3 = 'USA'
     GROUP BY address.state) r ON a.state = r.state
ORDER BY net_change ASC;
""")

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
