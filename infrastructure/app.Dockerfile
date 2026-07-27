# Single image for the whole modular monolith. Build context is the repo root.
FROM python:3.12-slim

WORKDIR /app

# Same reason as the Airflow image: multi-GB wheel downloads outlast pip's 15s
# default socket timeout, and a stall kills the whole layer.
ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

# transformers 4.x eagerly imports cv2 from image_utils.py, and the
# opencv-python that docling pulls in links against X11 libs that slim images
# don't ship. Without these: `ImportError: libxcb.so.1: cannot open shared
# object file`. (transformers is pinned <5 because jina-embeddings-v3's
# trust_remote_code module doesn't run on 5.x — see pyproject.toml.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends libxcb1 libgl1 libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
# --no-emit-project: uv export otherwise writes `-e .` into requirements.txt,
# but src/ is COPYed on the NEXT line, so the editable build has nothing to find.
# Deps first, source second — that ordering is what keeps the deps layer cached.
RUN uv export --frozen --no-dev --no-hashes --no-emit-project -o requirements.txt \
 && pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
