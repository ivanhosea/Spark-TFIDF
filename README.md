# Spark TF-IDF
Using Spark to compute Search Index that is built based on TF-IDF algorithm.
# Optimization Report
My main purpose here is to optimize the computation of this TF-IDF application. You can see the optimization report file in this repository: **TF-IDF with Spark - Optimization Report.pdf**.
## Dataset
The dataset is using a sample of total 9GB from Hugging Face wikipedia dataset: https://huggingface.co/datasets/wikipedia.
## Spark Requirements
- HDFS: for storing the dataset.
- JRE/JDK: Spark needs JVM to run.
- sbt: for building scala JAR file.
- datasets: Python library to load Hugging Face's Arrow files.
- PostgreSQL Driver: to load data to PostgreSQL
- SQLAlchemy & Pandas: to be able to run the search application.
## Files
- ingest_dataset.py: ingesting the original Hugging Face's Arrow dataset to HDFS. It works by first loading the file to python then transforming it into a Dataframe & loading it to HDFS.
- tf_idf.ipynb: the notebook containing Spark application that computes TF-IDF.
- TermFrequencyUDF.scala: the Scala code of the UDF used for calculating term frequency.
- db1.sql: containing PostgreSQL table structures & its indexes.
- load_to_postgres.ipynb: the notebook containing Spark application that loads TF-IDF result to PostgreSQL.
- search.py: a command line app that inputs words to be searched & outputs top 5 wiki pages.
## Implementation
1. First of all, dataset is ingested from source server to HDFS using Spark that is installed locally.
2. TF-IDF is then calculated with the already prepared Spark Cluster. The output of this will be stored back to HDFS as parquet files.
3. As an example usage, I load the parquet output to PostgreSQL.
4. After the load, I created index on both term_docs2 & doc_det2 tables to make search fast. This makes the query time goes from multiple seconds to just a few miliseconds.
5. The search application solely use document's term scores to calculate the top 5 pages, and that's why for the examples **search_result.png** some top results didn't contain the same term(s) as the searched phrase.