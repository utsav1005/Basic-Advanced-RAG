# Airflow workers import the ETL functions directly (src/services/ingestion),
# so this image needs the same deps as the app (docling, trafilatura,
# tiktoken, httpx, opensearch-py, sentence-transformers) minus the web
# layer AND minus sqlalchemy. src/ is mounted at runtime via docker-compose
# volumes; only deps are baked in here — same uv.lock the app image uses.
FROM apache/airflow:2.10.4-python3.12

# This image pulls ~3.5GB of wheels (torch, docling models, transformers). On a
# slow link pip's 15s default socket timeout aborts the whole layer partway
# through and the entire download restarts. Longer timeout + more retries.
ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

# transformers 4.x eagerly imports cv2 from image_utils.py, and the
# opencv-python that docling pulls in links against X11 libs the airflow image
# doesn't ship. Without these: `ImportError: libxcb.so.1: cannot open shared
# object file`. (transformers is pinned <5 because jina-embeddings-v3's
# trust_remote_code module doesn't run on 5.x — see pyproject.toml.)
# apt needs root; the rest of the build must run as the airflow user.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends libxcb1 libgl1 libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# /opt/airflow is the base image's WORKDIR, owned by the airflow user —
# /tmp is not writable by that user in this image, so build files here.
# --chmod=644: pyproject.toml is owner-only (rwx------) on the host, and
# COPY preserves that mode — without this, the non-root `airflow` user in
# the container can't even read the file it needs to export deps from.
COPY --chmod=644 pyproject.toml uv.lock ./
# Airflow 2.10.4 ships SQLAlchemy 1.4.54 internally and its own ORM models
# (TaskInstance etc.) do not import under 2.0.x. Excluding the direct
# `sqlalchemy` line from dag-requirements.txt is NOT enough — langchain-
# community/langchain-classic (pulled in transitively, unused by the DAGs
# themselves) declare their own sqlalchemy>=2.0 requirement, so pip's
# resolver reinstalls 2.x anyway to satisfy them. Fix: force-reinstall the
# exact version Airflow ships, as the LAST step, so it wins regardless of
# what earlier installs pulled in. Our DAG code only calls the 2.0-style
# execution API (engine.begin()/conn.execute(text(...))), which 1.4.54
# also supports, so this doesn't break anything on our side.
# --no-emit-project: uv export otherwise writes `-e .` (the project itself) into
# requirements.txt. Only pyproject.toml/uv.lock are COPYed here — src/ arrives at
# runtime via a compose volume — so pip's editable build has no package to find
# and fails with "Getting requirements to build editable did not run
# successfully". We want the DEPENDENCIES baked in, not the project.
RUN pip install --no-cache-dir uv \
 && uv export --frozen --no-dev --no-hashes --no-emit-project -o requirements.txt \
 && grep -viE '^(fastapi|uvicorn|python-multipart)' requirements.txt > dag-requirements.txt \
 && pip install --no-cache-dir -r dag-requirements.txt \
 && pip install --no-cache-dir "sqlalchemy==1.4.54" \
 && rm -f pyproject.toml uv.lock requirements.txt dag-requirements.txt
