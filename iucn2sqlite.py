import os
import sys

import pandas as pd
import sqlite3

TAXONOMY_TABLE = """
CREATE TABLE IF NOT EXISTS taxonomy(
	id INTEGER UNIQUE PRIMARY KEY,
	scientificName VARCHAR(255) NOT NULL,
	kingdomName VARCHAR(32) NOT NULL,
	phylumName VARCHAR(32) NOT NULL,
	orderName VARCHAR(32) NOT NULL,
	className VARCHAR(32) NOT NULL,
	familyName VARCHAR(255) NOT NULL,
	genusName VARCHAR(255) NOT NULL,
	speciesName VARCHAR(255) NOT NULL,
	infraType VARCHAR(255),
	infraName VARCHAR(255),
	infraAuthority VARCHAR(255),
	subpopulationName VARCHAR(255),
	authority VARCHAR(255),
	taxonomicNotes TEXT
)
"""
TAXONOMY_INDEXES = [
	"CREATE UNIQUE INDEX IF NOT EXISTS taxonomy_id_index ON taxonomy(id)"
]
TAXONOMY_INSERT = "INSERT INTO taxonomy VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

HABITAT_TABLE = """
CREATE TABLE IF NOT EXISTS habitat(
	id INTEGER UNIQUE PRIMARY KEY ASC,
	code VARCHAR(16) UNIQUE NOT NULL,
	name VARCHAR(255) NOT NULL,
	majorImportance INTEGER DEFAULT 0,
	season VARCHAR(32),
	suitability VARCHAR(255)
)
"""
HABITAT_INDEXES = [
	"CREATE UNIQUE INDEX IF NOT EXISTS habitat_id_index ON habitat(id)",
	"CREATE UNIQUE INDEX IF NOT EXISTS habitat_code_index ON habitat(code)"
]
HABITAT_INSERT = "INSERT INTO habitat(code, name, majorImportance, season, suitability) VALUES(?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"

TAXONOMY_HABITAT_M2M_TABLE = """
CREATE TABLE IF NOT EXISTS taxonomy_habitat_m2m(
	taxonomy INTEGER NOT NULL,
	habitat INTEGER NOT NULL
)
"""
TAXONOMY_HABITAT_M2M_INDEXES = [
	"CREATE INDEX IF NOT EXISTS taxonomy_habitat_m2m_taxonomy_index ON taxonomy_habitat_m2m(taxonomy)",
	"CREATE INDEX IF NOT EXISTS taxonomy_habitat_m2m_habitat_index ON taxonomy_habitat_m2m(habitat)"
]
TAXONOMY_HABITAT_M2M_INSERT = "INSERT INTO taxonomy_habitat_m2m VALUES(?, ?)"

COMMON_NAMES_TABLE = """
CREATE TABLE IF NOT EXISTS common_names (
	id INTEGER UNIQUE PRIMARY KEY ASC,
	taxonomy INTEGER NOT NULL,
	name VARCHAR(255) NOT NULL,
	language VARCHAR(32) NOT NULL,
	main INTEGER DEFAULT 0
)
"""
COMMON_NAMES_INDEXES = [
	"CREATE UNIQUE INDEX IF NOT EXISTS common_names_id_index ON common_names(id)",
	"CREATE INDEX IF NOT EXISTS common_names_taxonomy_index ON common_names(taxonomy)",
]
COMMON_NAMES_INSERT = "INSERT INTO common_names(taxonomy, name, language, main) VALUES(?, ?, ?, ?)"

try:
	dbname = sys.argv[1]
except KeyError:
	print(f"Usage: {sys.argv[0]} [DBNAME]")
	sys.exit(-1)

conn = sqlite3.connect(dbname)

conn.execute(TAXONOMY_TABLE)
for index in TAXONOMY_INDEXES:
	conn.execute(index)
conn.execute(HABITAT_TABLE)
for index in HABITAT_INDEXES:
	conn.execute(index)
conn.execute(TAXONOMY_HABITAT_M2M_TABLE)
for index in TAXONOMY_HABITAT_M2M_INDEXES:
	conn.execute(index)
conn.execute(COMMON_NAMES_TABLE)
for index in COMMON_NAMES_INDEXES:
	conn.execute(index)

for path in os.listdir():
	if not os.path.isdir(path):
		continue
	taxonmy_filename = os.path.join(path, 'taxonomy.csv')
	if not os.path.exists(taxonmy_filename):
		continue
	taxonomies = pd.read_csv(taxonmy_filename)
	for tuple in taxonomies.itertuples():
		res = conn.execute(TAXONOMY_INSERT, tuple[1:])
		assert res.rowcount == 1

	habitat_filename = os.path.join(path, 'habitats.csv')
	habitats = pd.read_csv(habitat_filename)
	for tuple in habitats.itertuples():
		res = conn.execute(HABITAT_INSERT, tuple[4:])

		# because of duplicates, the above line may not do an insert as the data was already there
		# and so we need to do a bit more work here
		lastrowid = res.lastrowid
		if res.rowcount == 0:
			res = conn.execute("select id from habitat where code==?", [tuple[4]]).fetchall()
			assert len(res) == 1
			lastrowid = res[0][0]

		res = conn.execute(TAXONOMY_HABITAT_M2M_INSERT, (tuple[2], lastrowid))
		assert res.rowcount == 1

	common_names_filename = os.path.join(path, 'common_names.csv')
	common_names = pd.read_csv(common_names_filename)
	for tuple in common_names.itertuples():
		res = conn.execute(COMMON_NAMES_INSERT, tuple[1:2] + tuple[3:])
		assert res.rowcount == 1

conn.commit()
conn.close()
