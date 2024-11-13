# Project Plan: Jail Deaths Dataset Analysis

## Title  
Jail Deaths in the U.S.: An Analysis of Trends and Causes

## Main Question  
What are the trends and contributing factors to jail deaths across different U.S. states?

## Description  
This project will analyze a dataset of jail deaths in the U.S., focusing on trends related to cause of death, demographics, and state-level differences. The goal is to identify patterns, understand potential systemic issues, and provide insights into the effectiveness of jail health care and the conditions within U.S. jails.

## Datasources  
- **Datasource**: Jail Deaths Dataset (ZIP file with Excel)  
  - **URL**: [Jail Deaths Dataset](https://graphics.thomsonreuters.com/data/jails/Allstatesinsurvey.zip)  
  - **Description**: Data on in-custody deaths across U.S. jails, including cause, date, state, and other demographics.

## Work Packages  

- **Work Package #1**: **Data Collection**  
  - **Objective**: Download, extract, and load the dataset from the provided URL.  
  - **Deliverable**: Loaded data into a pandas DataFrame from the extracted Excel file within the ZIP archive.

- **Work Package #2**: **Data Cleaning**  
  - **Objective**: Clean missing values, remove duplicates, and ensure correct data types.  
  - **Deliverable**: A cleaned and processed DataFrame, with all necessary transformations.

- **Work Package #3**: **Exploratory Data Analysis (EDA)**  
  - **Objective**: Identify key trends, such as death rates over time, cause of death, and state-level variations.  
  - **Deliverable**: EDA results with visualizations and summary statistics.

- **Work Package #4**: **Data Storage**  
  - **Objective**: Store cleaned data in both SQLite and CSV formats for further analysis.  
  - **Deliverable**: SQLite database containing cleaned data and a CSV file saved to the `/data` directory.

## Next Steps  
- Optional: Perform deeper statistical analyses or implement predictive models on trends observed.  
- Refine analysis as new insights emerge from the dataset.
