# Spotify Analytics Data Pipeline

A scalable, serverless data pipeline that extracts and processes Spotify music data for analytics purposes. This project demonstrates modern data engineering practices using AWS services and implements automated data extraction, processing, and analysis capabilities.

## Architecture Overview

![Architecture](https://github.com/user-attachments/assets/c541b891-2e97-4876-b380-2a4282003b6e)


### Core AWS Services

#### Storage & Processing
- **Amazon S3**: Serves as our data lake, organizing data into raw and transformed layers
  - Raw zone: Stores unprocessed API responses
  - Transformed zone: Contains cleaned, normalized data ready for analysis
  
#### Compute & Orchestration
- **AWS Lambda**:
  - Extraction Lambda: Interfaces with Spotify API to fetch raw data
  - Transformation Lambda: Processes raw data into analytics-ready formats
- **Amazon CloudWatch**: Manages workflow orchestration through scheduled triggers

#### Data Discovery & Analytics
- **AWS Glue Crawler**: Automatically discovers and catalogs data schema
- **AWS Glue Data Catalog**: Central metadata repository
- **Amazon Athena**: Enables serverless SQL analytics on S3 data

## Data Flow

1. **Data Extraction**:
   - CloudWatch triggers extraction Lambda on a weekly schedule
   - Lambda authenticates with Spotify API
   - Fetches latest music data (albums, artists, tracks)
   - Stores raw JSON responses in S3 raw zone

2. **Data Processing**:
   - Transformation Lambda processes raw data
   - Implements data quality checks
   - Performs necessary transformations
   - Saves processed data in Parquet format to transformed zone

3. **Data Discovery**:
   - Glue Crawler indexes transformed data
   - Updates Data Catalog with latest schema
   - Makes data queryable via Athena

4. **Analytics**:
   - Athena provides SQL interface for ad-hoc analysis
   - Supports complex queries across multiple datasets
   - Enables creation of dashboards and reports

## Dependencies

```plaintext
pandas==2.0.0
boto3==1.26.137
spotipy==2.23.0
numpy==1.24.3
```

Thanks for reading
