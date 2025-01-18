# YouTube Data Harvesting and Warehousing using MySQL, MongoDB, and Streamlit

LinkedIn Profile : [Kalai Selvam](https://www.linkedin.com/in/kalai-selvam-55428126a)

## Problem Statement
Create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

---

## Tools and Libraries Used

1. **Streamlit**  
   Streamlit was used to create a user-friendly UI for interacting with the application and performing data retrieval and analysis.

2. **Python**  
   Python, known for its simplicity and versatility, is the primary language used for developing this application, including data retrieval, processing, analysis, and visualization.

3. **Google API Client**  
   The `googleapiclient` library facilitates interaction with YouTube Data API v3, enabling the retrieval of channel details, video specifics, and comments.

4. **MongoDB**  
   MongoDB, a document database, is used for flexible and scalable storage of structured and unstructured data in a JSON-like format.

5. **MySQL**  
   MySQL, an open-source relational database management system, is used for managing structured data efficiently and supporting powerful SQL queries.

---

## Required Libraries

- pip install google-api-python-client
- pip install streamlit
- pip install mysql-connector-python
- pip install pymongo
- pip install pandas


## Features

1. **Data Retrieval**  
   Retrieve YouTube channel and video data via the YouTube Data API.

2. **Data Storage**  
   Store retrieved data in a MongoDB database (data lake) for flexible and scalable storage.

3. **Data Migration**  
   Migrate data from MongoDB to a MySQL database for efficient querying and analysis.

4. **Data Search and Retrieval**  
   Use SQL queries to search and retrieve YouTube data from the MySQL database, offering various search options.

---
Overall, the code provides a full cycle of data handling, starting from extraction from YouTube, warehousing in MongoDB, transformation, then moving to MySQL, and finally, offering a querying interface through Streamlit.
