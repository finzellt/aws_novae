# Dockerfile at repo root
FROM public.ecr.aws/lambda/python:3.11-arm64
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy shared code
COPY common/ /opt/python/common/
COPY shared/nova_schema /opt/python/nova_schema
ENV PYTHONPATH="/var/task:/opt/python"


RUN python - <<'PY'
import sys, importlib
print("sys.path:", sys.path)
m = importlib.import_module("nova_schema")
print("nova_schema file:", getattr(m, "__file__", None))
importlib.import_module("nova_schema.models")
importlib.import_module("nova_schema.mapping")
print("nova_schema imports OK")
PY


# ARG to choose which handler to copy
ARG HANDLER_PATH=src/resolve_simbad_metadata/app.py
COPY ${HANDLER_PATH} ${LAMBDA_TASK_ROOT}/app.py
CMD ["app.handler"]