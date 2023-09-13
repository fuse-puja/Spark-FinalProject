# Spark-FinalProject

## Brazilian E-commerce Dataset Analysis
This repository contains code for analyzing the Brazilian E-commerce dataset using PySpark. The dataset used in this analysis is available on Kaggle.
---
## Introduction
The analysis involves cleaning and processing the dataset and answering two key questions:

- Delivery and Purchase Trends Analysis:
  - Compute the delivery deviation in days.
  - Calculate the actual delivery time.
  - Categorize the order purchase hour as Dawn, Morning, Afternoon, or Night.
    
- Review Sentiment Analysis:
  - Perform sentiment analysis on customer reviews.
  - Determine the correlation between sentiment scores and review scores.
---

## Prerequisites
Before running the code, ensure you have the following prerequisites installed:

- PySpark
- PostgreSQL
- Python libraries: textblob
---
## Code Structure
The code is organized as follows:

- Initializing Spark: Setting up the Spark session.

- Loading the Dataset: Loading various CSV files from the dataset into Spark DataFrames.

- Data Cleaning and Pre-processing: Performing data cleaning operations, including removing whitespace, replacing values, and handling null values.

- Storing in Parquet Files: Storing the cleaned DataFrames in Parquet format for future use.

- Writing the Clean DataFrames to PostgreSQL: Saving the cleaned DataFrames to a PostgreSQL database.

- Reading from PostgreSQL: Reading DataFrames from PostgreSQL for further analysis.

- Question 1: Delivery and Purchase Trends Analysis: Analyzing delivery trends and purchase trends on a daily basis.

- Question 2: Review Sentiment Analysis: Analyzing the sentiment of customer reviews and finding the correlation between sentiment scores and review scores.

- Storing Final Results in PostgreSQL: Saving the results of the analysis in PostgreSQL.
---
## Running the Code
To run the code, follow these steps:

- Download the dataset from the Kaggle link and place the CSV files in the ./Data directory.

- Make sure you have a PostgreSQL database set up and configure the PostgreSQL connection parameters in the code.

- Run the code using a Python environment with PySpark and the required libraries installed.
---
## Results
The results of the analysis are stored in the PostgreSQL database and include information about delivery trends, purchase trends, sentiment analysis of reviews, and the correlation between sentiment scores and review scores.
