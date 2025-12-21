FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency file first (enables Docker caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY src/ src/
COPY data/ data/

# Default command (can be overridden)
CMD ["python"]
