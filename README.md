**"YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit"**

The objective of this project is to develop a user-friendly Streamlit UI that integrates with the YouTube API. The application allows users to explore and analyze data from multiple YouTube channels by entering a YouTube channel ID. The relevant data, such as channel name, subscribers, video count, playlist ID, video ID, likes, dislikes, and comments, is retrieved from the YouTube API using Google API. 
The data is then stored in a MongoDB database, serving as a data lake for easy access and storage. The application supports collecting data for up to 10 different YouTube channels, and users can initiate the data collection process by clicking a button.
To enhance data management and querying capabilities, the collected data for a selected channel can be migrated from the data lake to a SQL database. This migration process organizes the data into tables, enabling efficient querying and analysis using SQL.
Within the Streamlit app, users can perform various searches and retrieve data from the SQL database using different search options. The application supports advanced functionality, such as joining tables to obtain comprehensive channel details.

**Approach**

The overall approach for this project includes 

1.setting up the Streamlit app, 

2.establishing a connection to the YouTube API,

3.storing data in a MongoDB data lake,

4.migrating the data to a SQL data warehouse,

5.executing SQL queries on the data warehouse, and finally,

6.presenting the results within the Streamlit app for a seamless user experience.
