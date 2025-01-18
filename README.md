# Youtube_Data_Harvesting_and_Warehousing_using-MySQL_MongoDB_Streamlit

Linked in URL : https://www.linkedin.com/in/kalai-selvam-55428126a

Problem Statement: Create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

TOOLS AND LIBRARIES USED :

1. Streamlit
Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

3. Python
Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

4. Google API Client
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

5. MongoDB
MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

6. MySQL
MySQL is an open-source relational database management system known for its speed, reliability, and ease of use. It is widely used for managing structured data in a wide range of applications, from small websites to large-scale enterprise solutions. MySQL supports a variety of data types, offers efficient indexing, and provides a powerful query language (SQL) for data manipulation.


REQUIRED LIBRARIES :

1.googleapiclient.discovery

2.streamlit

3.mysql

4.pymongo

5.pandas


FEATURES :

Data Retrieval: Retrieve YouTube channel and video data via the YouTube Data API.

Data Storage: Store retrieved data in a MongoDB database (data lake) for flexible and scalable storage.

Data Migration: Migrate data from MongoDB to a MySQL database for more efficient querying and analysis.

Data Search and Retrieval: Use SQL queries to search and retrieve YouTube data from the MySQL database, offering various search options.



Overall, the code provides a full cycle of data handling, starting from extraction from YouTube, warehousing in MongoDB, transformation, then moving to MySQL, and finally, offering a querying interface through Streamlit.
