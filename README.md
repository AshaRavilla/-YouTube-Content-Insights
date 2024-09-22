# Trending Youtube Data Analytics | Modern Data Engineering AWS Project

## Introduction
This project, Trending YouTube Video Data Analytics | Modern Data Engineering AWS Project, focuses on building a data pipeline to ingest trending YouTube video data via the YouTube API. It leverages AWS services like Lambda, S3, and Glue ETL to automate the process of storing, cleansing, and analyzing the data. The project demonstrates how to efficiently manage data ingestion, transformation, and querying in a scalable cloud environment.

## Architechture
![Project Architechture](architecture.jpeg)

## Technology Used
1. Programming Language - Python, Pyspark
2. Scripting Language - SQL
3. AWS cloud Platform
   - Simple Storage Service
   - AWS Lambda
   - AWS Glue
   - Idendity Access Management
   - Quick Sight

## Dataset used
The dataset used in this project is sourced directly from the YouTube Data API, which provides real-time information about trending videos across different regions. The data includes various attributes of trending videos, such as video title, description, publish date, view count, like count, comment count, and category ID.

In addition to video-specific data, category metadata is also included, allowing for insights into the type of content trending in different regions. You can find more information about the video and category data attributes from the YouTube Data API Documentation.  [YouTube Data API Documentation](https://developers.google.com/youtube/v3/docs).

The raw data is ingested daily, stored in Amazon S3, and processed to extract meaningful analytics, such as the most popular video categories, top content creators, and regional trends over time.

## Scripts for Project
1.[Extract.py](src/Data_ingestion.py)
2.[Transformation_Category_data.py](src/Data_Transformation_category_JSON_to_Parquet.py)
3.[Transformation_video_data.py](src/Data_Transformation_CSV_parquet.py)
4.[Load.py](src/video_category_join.py)

