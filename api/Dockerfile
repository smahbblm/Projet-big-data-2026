FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

# install dependencies (NO pip upgrade)
RUN pip3 install --break-system-packages -r requirements.txt

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]