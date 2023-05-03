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
	taxonomicNotes TEXT,

	assessment INTEGER NOT NULL DEFAULT 0,
	elevationLower INTEGER NOT NULL DEFAULT -500,
	elevationUpper INTEGER NOT NULL DEFAULT 9000
)
"""
TAXONOMY_INDEXES = [
	"CREATE UNIQUE INDEX IF NOT EXISTS taxonomy_id_index ON taxonomy(id)"
]
TAXONOMY_INSERT = "INSERT INTO taxonomy VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

HABITAT_TABLE = """
CREATE TABLE IF NOT EXISTS habitat(
	id INTEGER UNIQUE PRIMARY KEY ASC,
	code VARCHAR(16) UNIQUE NOT NULL,
	name VARCHAR(255) NOT NULL
)
"""
HABITAT_INDEXES = [
	"CREATE UNIQUE INDEX IF NOT EXISTS habitat_id_index ON habitat(id)",
	"CREATE UNIQUE INDEX IF NOT EXISTS habitat_code_index ON habitat(code)"
]
HABITAT_INSERT = "INSERT INTO habitat(code, name) VALUES(?, ?) ON CONFLICT DO NOTHING"

TAXONOMY_HABITAT_M2M_TABLE = """
CREATE TABLE IF NOT EXISTS taxonomy_habitat_m2m(
	taxonomy INTEGER NOT NULL,
	habitat INTEGER NOT NULL,
	majorImportance INTEGER NOT NULL DEFAULT 0,
	season VARCHAR(32) NOT NULL DEFAULT "Seasonal Occurrence Unknown",
	suitability VARCHAR(32) NOT NULL DEFAULT "Unknown"
)
"""
TAXONOMY_HABITAT_M2M_INDEXES = [
	"CREATE INDEX IF NOT EXISTS taxonomy_habitat_m2m_taxonomy_index ON taxonomy_habitat_m2m(taxonomy)",
	"CREATE INDEX IF NOT EXISTS taxonomy_habitat_m2m_habitat_index ON taxonomy_habitat_m2m(habitat)"
]
TAXONOMY_HABITAT_M2M_INSERT = "INSERT INTO taxonomy_habitat_m2m VALUES(?, ?, ?, ?, ?)"

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
		row = tuple[1:] + (0, -500, 9000)
		res = conn.execute(TAXONOMY_INSERT, row)
		assert res.rowcount == 1

	habitat_filename = os.path.join(path, 'habitats.csv')
	habitats = pd.read_csv(habitat_filename)
	for tuple in habitats.itertuples():
		res = conn.execute(HABITAT_INSERT, tuple[4:6])

		# because of duplicates, the above line may not do an insert as the data was already there
		# and so we need to do a bit more work here
		lastrowid = res.lastrowid
		if res.rowcount == 0:
			res = conn.execute("select id from habitat where code==?", [tuple[4]]).fetchall()
			assert len(res) == 1
			lastrowid = res[0][0]

		# As per comment from Daniele in iucn_modlib, the batch data from IUCN has hygine issues, so
		# we need to do some tidying on import to make it more like the API responses
		importance = tuple[6]
		if importance not in ['Yes', 'No']:
			importance = 'No'

		season = tuple[7]
		season_mapping = {
			'passage': 'Passage',
			'resident': 'Resident',
			'breeding': 'Breeding Season',
			'non-breeding': 'Non-Breeding Saeson',
			'unknown': 'Seasonal Occurrence Unknown'
		}
		if season in season_mapping:
			season = season_mapping[season]
		if season not in ['Passage', 'Resident', 'Breeding Season', 'Non-Breeding Season', 'Seasonal Occurrence Unknown']:
			print(season)
			season = 'Seasonal Occurrence Unknown'

		suitability = tuple[8]
		if suitability not in ["Suitable", "Marginal", "Unknown"]:
			suitability = "Unknown"

		res = conn.execute(TAXONOMY_HABITAT_M2M_INSERT, (tuple[2], lastrowid, importance == 'Yes', season, suitability))
		assert res.rowcount == 1

	other_data_filename = os.path.join(path, 'all_other_fields.csv')
	other_data = pd.read_csv(other_data_filename)
	for _, tuple in other_data.iterrows():
		species_id = tuple['internalTaxonId']

		try:
			assessment = int(tuple['assessmentId'])
			res = conn.execute("UPDATE taxonomy SET assessment = ? WHERE id == ?", (assessment, species_id))
			assert res.rowcount == 1
		except ValueError:
			pass

		try:
			elevation_lower = int(tuple['ElevationLower.limit'])
			elevation_upper = int(tuple['ElevationUpper.limit'])
			if elevation_lower < elevation_upper:
				elevation_lower = max(-500, elevation_lower)
				elevation_upper = min(9000, elevation_upper)
				res = conn.execute("UPDATE taxonomy SET elevationLower = ?, elevationUpper = ? WHERE id == ?", (elevation_lower, elevation_upper, species_id))
				assert res.rowcount == 1
		except ValueError:
			pass


	common_names_filename = os.path.join(path, 'common_names.csv')
	common_names = pd.read_csv(common_names_filename)
	for tuple in common_names.itertuples():
		res = conn.execute(COMMON_NAMES_INSERT, tuple[1:2] + tuple[3:])
		assert res.rowcount == 1

conn.commit()
conn.close()
