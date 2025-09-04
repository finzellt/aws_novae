# Dockerfile at repo root
FROM public.ecr.aws/lambda/python:3.11-arm64
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy shared code
COPY common/ /opt/python/common/
ENV PYTHONPATH="/var/task:/opt/python"

# ARG to choose which handler to copy
ARG HANDLER_PATH=src/resolve_simbad_metadata/app.py
COPY ${HANDLER_PATH} ${LAMBDA_TASK_ROOT}/app.py
CMD ["app.handler"]