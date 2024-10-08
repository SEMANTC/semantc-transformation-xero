FROM python:3.9-slim

WORKDIR /usr/app/dbt

# install dbt and other dependencies
RUN pip install dbt-bigquery

# copy dbt project files
COPY ./dbt .

# copy the startup script
COPY start.sh .
RUN chmod +x start.sh

# set environment variables (to be overridden at runtime)
ENV TENANT_ID=""
ENV PROJECT_ID=""
ENV PORT=8080

# Run the startup script
CMD ["./start.sh"]