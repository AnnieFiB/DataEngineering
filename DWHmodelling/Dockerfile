FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Make the start script executable
RUN chmod +x start.sh

# Run the shell script
CMD ["./start.sh"]
