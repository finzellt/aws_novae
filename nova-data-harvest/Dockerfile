# Base image with AWS Lambda Runtime for Python 3.11
FROM public.ecr.aws/lambda/python:3.11-arm64

# Copy and install requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt -t "${LAMBDA_TASK_ROOT}"

# Copy function source
COPY src/init_context ${LAMBDA_TASK_ROOT}

# Set handler (also set in template ImageConfig)
CMD ["app.handler"]
