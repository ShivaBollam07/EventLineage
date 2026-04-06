FROM python:3.12-slim
WORKDIR /app

# Install SDK first to leverage layer caching
COPY sdk/python /app/sdk/python
RUN pip install --no-cache-dir ./sdk/python

# Install processor dependencies
COPY services/dicom-processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/dicom-processor/app.py .

EXPOSE 5002
CMD ["python", "app.py"]

