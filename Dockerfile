# Use lightweight Python
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Give permission to entrypoint script
RUN chmod +x entrypoint.sh

# Expose Flask port
EXPOSE 5000

# Run entrypoint
CMD ["./entrypoint.sh"]