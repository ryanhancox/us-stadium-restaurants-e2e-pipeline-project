# US Stadium Restaurants E2E Data Pipeline Project

## Introduction
Hello and welcome to my end-to-end data pipeline project using Azure, Snowflake, dbt, and Apache Airflow! 

This project uses nested JSON data from Kaggle which can be found [here](https://www.kaggle.com/datasets/xavier4t/sports-and-restaurants). The data contains restaurant data for restaurants that are within a given radius of a US stadium. It also contains the teams and the (sports) leagues that those teams play in for each stadium. Please follow the link for the full description of the dataset.

There are two folders for the data - one called 'Category' and one called 'General'. For this project I have used the data in the 'Category' folder, but either could have been used.

## Project Architecture

![Project Architecture](/images/us_stadium_restaurants_architecture.jpeg)

To summarise:
1. The Kaggle data is downloaded using the Python script `kaggle_download.py`
2. The downloaded data is then uploaded to **Azure Blob Storage** using the Python script `azure_upload.py` and credentials that are in a hidden `.env` file
3. The **Azure Blob Storage** data is then loaded into **Snowflake** using a **Snowflake External Stage** and a **Snowflake Task using the COPY INTO command** - the Snowflake task has a 5 minute cadence to frquently pick up any new files that land in the Azure Blob Storage.
4. A seperate `state_codes.csv` reference file has been manually uploaded to **Snowflake** for data cleansing purposes
4. **dbt** is then used to transform the data through the data warehouse medallion architecture. Extensive tests have also been implemented using dbt yaml files.
5. **Apache Airflow** is used to orchestrate and execute the **dbt DAG** through the use of **astronomer-cosmos** - the DAG is scheduled to run at midnight each day
6. The resulting "conformed" data is used to create a **Snowflake Dashboard**

## Data Models
Since the data is not transactional in nature, it would not have been appropriate to attempt to do regular analytical data modelling - such as using a **star schema**. 

Additionally, as this data will ultimately be used for analytics, it would not be practical to model it in **Third Normal Form (3NF)**. 

As such, the data has only been modelled in **Second Normal Form (2NF)** so that less table joins are required when using the "conformed" data for analysis and visualisation.

### Conceptual Data Model

![Conceptual Data Model](/images/us_stadium_restaurants_conceptual.jpg)

### Logical Data Model

![Logical Data Model](/images/us_stadium_restaurants_logical.jpg)

## DAG
The resulting **Apache Airflow DAG** dag for the data transformations from the `raw` schema to the `conformed` schema:

![Apache Airflow DAG](/images/airflow_dag.png)

## Snowflake Dashboard

Before dashboarding, it was important to think of a use-case for the data and the value that it could bring. The main use-case that came to mind was:  

* **Affiliate marketing for restaurants near stadiums** - stadiums can offer bundles/discounts for local restaurants/bars when fans purchase tickets.

Admittedly, more data would be required to do this successfully. However, the data could give us a good idea of which stadiums would benefit the most from this.

Given the above, what questions do we want the data to answer. Here's a few:

* Which stadiums have the most sports teams that play there? More teams playing at a stadium = more custom = more affiliate marketing potential.
* Which stadiums are surrounded by the greatest amount of restaurant/bars? More restaurants/bars in stadium vicinity = more marketing potential.
* What are the most common categories of establishment surrounding stadiums? I.e. you would typically expect to see more bars surrounding stadiums as they receive the most custom -> focus marketing on these businesses.
* What price bracket do establishments that surround stadiums most typically fall into? If there is more of one price bracket, it is probably because they get more custom -> focus marketing for on these businesses.

Given the above questions, the following Snowflake dashboard was produced by querying the data in the "conformed" layer of the data warehouse:

![Snowflake Dashboard](/images/us-stadium-restaurants-snowflake-dashboard.png)

**Note that this data analysis has purposely been left fairly basic as the main focus of this project was on the ETL process and data modelling.**

## Notes
1. Ideally, instead of two Python scripts, I would have liked an Azure Function to perform the data ingest from Kaggle to Blob Storage. Unfortunately downloading from the Kaggle API requires a `.kaggle` file containing your credentials to be stored in your user directory on your machine. Whilst I could have looked to deploy a docker container with the required Python script and credentials to a virtual machine, this felt like overkill for this project. 

## Improvements (To-Do)
* Determine a practical method to ingest data into Azure Blob Storage without having to intermediately having to store it on your local machine (file share?).
* Use a Snowflake Snowpipe for continuous data loading from Azure Blob Storage
* Use a dbt seed file for the `state_code.csv` reference data instead of manually uploading into Snowflake
* Wrap project into a Docker container and deploy to an Azure VM
* Look to use a better data visualisation tool (sorry Snowflake) for the data analysis and visualisation. Create a variety of visualisation including geographic visualisation using the geographic data available in the dataset.