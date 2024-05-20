import pandas as pd
from sqlalchemy import create_engine, text
import sys
import configparser

# Read Config
config = configparser.ConfigParser()
config.read('search_config.properties')
USERNAME = config.get('search', 'username')
PASSWORD = config.get('search', 'password')
HOST = config.get('search', 'host')

# Parse Input
words = [f"'{w.lower()}'" for w in sys.argv[1:]]
q_words = ', '.join(words)

# Connection String
connection_string = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:5432/db1'

# Create SQL Alchemy Engine
engine = create_engine(connection_string)

# Ranking Documents Query
query = f'''
WITH cte AS (
SELECT unnested_data.doc_id AS doc_id, sum(unnested_data.ctr) * POWER(COUNT(1), 3) AS score
FROM term_docs2, UNNEST(docs) AS unnested_data
WHERE term IN ({q_words})
GROUP BY unnested_data.doc_id
ORDER BY 2 DESC
LIMIT 5
)
SELECT d.doc_id, d.title, d.url, cte.score
FROM doc_det2 d
JOIN cte ON d.doc_id = cte.doc_id
'''

with engine.connect() as connection:
    df = pd.read_sql(text(query), connection)
    print(df.head())