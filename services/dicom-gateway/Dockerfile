FROM python:3.12-slim
WORKDIR /app

# Install SDK first to leverage layer caching
COPY sdk/python /app/sdk/python
RUN pip install --no-cache-dir ./sdk/python

# Install gateway dependencies
COPY services/dicom-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/dicom-gateway/app.py .

EXPOSE 5001
CMD ["python", "app.py"]

