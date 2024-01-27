services:
  pgdatabase:
    image: postgres:14
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data"
    healthcheck:
        test: ["CMD", "pg_is_ready", "-U", "ny_taxi"]
        interval: 5s,
        retries: 5
    restart: always

docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:14
    
