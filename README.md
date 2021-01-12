# Data Warehouse Design

## Project Overview
In this project, I architected, designed and staged a data warehouse to assess if weather has an any effect on customer reviews of restaurants.

## Dataset
I used data from Yelp [website](https://www.yelp.com/dataset/download) and the Global Historical Climatology Network-Daily (GHCN-D) [database](https://crt-climate-explorer.nemac.org/).

## Project Steps
1.	Create a data architecture diagram to visualize how to ingest and migrate the data into Staging, Operational Data Store (ODS), and Data Warehouse evironments, so as to ultimately query the data for relationships between weather and Yelp reviews.
2.	Create a staging environment(schema) in Snowflake.
3.	Upload all Yelp and Climate data to the staging environment. (Screenshots 1,2)
4.	Create an ODS environment(aka schema).
5.	Draw an entity-relationship (ER) diagram to visualize the data structure.
6.	Migrate the data into the ODS environment. (Screenshots 3,4,5,6)
7.	Draw a STAR schema for the Data Warehouse environment.
8.	Migrate the data to the Data Warehouse. (Screenshot 7)
9.	Query the Data Warehouse to determine how weather affects Yelp reviews. ( Screenshot 8)

Description of screenshots
1.	Screenshot of 6 tables created upon upload of YELP data
2.	Screenshot of 2 tables created upon upload of climate data
3.	SQL queries code that transforms staging to ODS. (include all queries)
4.	SQL queries code that specifically uses JSON functions to transform data from a single JSON structure of staging to multiple columns of ODS. (can be similar to #3, but must include JSON functions)
5.	Screenshot of the table with three columns: raw files, staging, and ODS. (and sizes)
6.	SQL queries code to integrate climate and Yelp data
7.	SQL queries code necessary to move the data from ODS to DWH.
8.	SQL queries code that reports the business name, temperature, precipitation, and ratings.
