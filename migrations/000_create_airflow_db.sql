-- Runs before 001_init.sql (alphabetical). Airflow keeps its metadata in the
-- same Postgres instance, in its own `airflow` database — no second DB
-- container to operate. pgvector/rag tables stay in `ragdb`.
CREATE DATABASE airflow;
