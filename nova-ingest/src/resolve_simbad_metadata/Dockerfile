# Use the Python 3.11 Lambda base
FROM public.ecr.aws/lambda/python:3.11-x86_64

# System deps for numpy/scipy/astropy (kept minimal)
RUN yum -y update && yum -y install \
    gcc gcc-c++ make \
    libgfortran \
    openblas-devel \
    && yum clean all && rm -rf /var/cache/yum

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}/

# Set handler
CMD [ "app.handler" ]
