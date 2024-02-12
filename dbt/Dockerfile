# Use the official dbt Docker image for BigQuery
FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.2

# Set the working directory to /usr/app/dbt
WORKDIR /usr/app/dbt

# Copy the content of the local ./cmc_elt directory to /usr/app/dbt
COPY ./cmc_elt /usr/app/dbt

# Copy the content of the local ./.dbt directory to /root/.dbt
COPY ./.dbt /root/.dbt