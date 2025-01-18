from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import datetime
import re
import streamlit as st


#API KEY connection
def Api_connect():
    Api_id="AIzaSyB1zmAjdfBYvBHZCtTPO0DVg7CwK_FsFqw"
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_id)

    return youtube

youtube=Api_connect()

# Get Channels Information
def get_channel_info(channel_id):

    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics", # Requesting basic channel info, content details, and statistics
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        # Extracting and storing key channel data
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        
    return data


#Get Video IDs
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(
        id=channel_id,
        part="contentDetails" # Fetch content details for the channel
    ).execute()
    
    Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
            part='snippet', # Requesting the snippet part of video details to get the video IDs
            playlistId=Playlist_Id,
            maxResults=50, # Fetching a maximum of 50 results per request
            pageToken=next_page_token  # Using pagination if there are more than 50 videos
            ).execute()
        
        # Loop through the fetched videos to collect their IDs
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken') # Fetch the next page token to continue pagination
        if next_page_token is None:
            break  # Exit the loop when there are no more pages to fetch

    return video_ids


#Get Video Information
def get_video_info(video_Id):
    video_data=[]
    for vid in video_Id:
        request=youtube.videos().list(
            id=vid,
            part="contentDetails,snippet,statistics" # Requesting video content, snippet, and statistics details for each video
            )
        response=request.execute()
        
        for item in response['items']:
            # Collecting and structuring video details into a dictionary
            data=dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
                )
            video_data.append(data)

    return video_data


#get comment information
def get_comment_info(video_Id):
    Comment_data=[]
    try:
        for vid in video_Id:
            request=youtube.commentThreads().list(
                videoId=vid,
                part="snippet", # Requesting the snippet part to fetch comment details
                maxResults=50 # Limiting the number of comments to 50 per request
                )
            response=request.execute()

            # Loop through the comment threads and extract comment data
            for item in response['items']:
                data=dict(
                        data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])              
                    )
                Comment_data.append(data)
    except:
        pass # Handle any errors (e.g., comment section turned off, No comments) silently

    return Comment_data


#get playlist information
def get_playlist_details(channel_id):
        next_page_token=None
        Playlist_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails', # Requesting snippet and content details of playlists
                        channelId=channel_id,
                        maxResults=50, # Fetching a maximum of 50 playlists per request
                        pageToken=next_page_token # Using the pagination token if available
                )
                response=request.execute()

                # Looping through the playlists to extract necessary details
                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        Playlist_data.append(data)

                next_page_token=response.get('nextPageToken') # Checking if there is a next page to continue fetching
                if next_page_token is None:
                        break # Exit the loop when no more pages are available
                
        return Playlist_data


# MongoDB connection setup
client=pymongo.MongoClient("mongodb+srv://kalai:kalai@cluster0.k4osw7u.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"] # Selecting the 'Youtube_data' database in MongoDB
coll1=db["channel_details"] # Selecting the 'channel_details' collection within the database

# MySQL connection setup
mydb=mysql.connector.connect(host="localhost",user='kalai',password="kalai",database="kalaidb")
cursor=mydb.cursor() # Creating a cursor object to execute SQL queries


# Function to upload channel details to MongoDB collection
def channel_details(channel_id):
    # Fetching channel-related data using previously defined funtions
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    # Uploading the fetched data to MongoDB
    coll1=db["channel_details"] # Selecting the 'channel_details' collection
    coll1.insert_one({"channel_information":ch_details, # Storing channel information
                      "playlist_information":pl_details, # Storing playlist information
                      "video_information":vi_details, # Storing video information
                      "comment_information":com_details # Storing comment information
                      })
    
    return "upload completed successfully"


# Connect to MySQL and creatE Tables

# Create channels table in MySQL
def channels_table(single_ch_name1):

    # Convert MongoDB channel info document to a DataFrame
    coll1=db["channel_details"]
    s_ch_list=[]

    # Fetching the channel data from MongoDB for a specific channel
    for result in coll1.find({"channel_information.Channel_Name":single_ch_name1},{"_id":0}):
        s_ch_list.append(result["channel_information"])

    channel_df=pd.DataFrame(s_ch_list)
    
    # Create the 'channels' table if it doesn't exist
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                            Channel_Id varchar(80) primary key,
                                            Subscribers bigint,
                                            Views bigint,
                                            Total_Videos int,
                                            Channel_Description text,
                                            Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()

    # Insert the data into the 'channels' table
    for index,row in channel_df.iterrows():
        insert_query='''insert into channels values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=[row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id']]
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            # If insertion fails, return a message indicating the channel already exists
            news=f"Provided Channel : {single_ch_name1} Already Exists in SQL"
            return news




# Create playlists table in MySQL
def playlists_table(single_ch_name1):

    # Create the 'playlists' table if it doesn't exist
    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                            Title varchar(200),
                                            Channel_Id varchar(100),
                                            Channel_Name varchar(100),
                                            PublishedAt varchar(50),
                                            Video_Count int)'''

    cursor.execute(create_query)
    mydb.commit()

    # Convert MongoDB playlist information document to DataFrame
    coll1=db["channel_details"]
    s_ch_list=[]
    for result in coll1.find({"channel_information.Channel_Name":single_ch_name1},{"_id":0}):
        s_ch_list.append(result["playlist_information"])

    playlist_df=pd.DataFrame(s_ch_list[0])

    # Insert playlist data into the 'playlists' table
    for index,row in playlist_df.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )

        cursor.execute(insert_query,values)
        mydb.commit()


# Create videos table in MySQL
def videos_table(single_ch_name1):

    # Create the 'videos' table if it doesn't exist    
    create_query='''create table if not exists videos(Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(50) primary key,
            Title VARCHAR(150),
            Tags VARCHAR(5000),
            Thumbnail VARCHAR(200),
            Description VARCHAR(10000),
            Published_Date TIMESTAMP,
            Duration TIME,
            Views BIGINT,
            Likes BIGINT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(100),
            Caption_Status VARCHAR(100))'''

    cursor.execute(create_query)
    mydb.commit()

    # Convert MongoDB video info document to DataFrame
    coll1=db["channel_details"]
    s_ch_list=[]
    for result in coll1.find({"channel_information.Channel_Name":single_ch_name1},{"_id":0}):
        s_ch_list.append(result["video_information"])

    video_df=pd.DataFrame(s_ch_list[0])

    # Insert video data into the 'videos' table
    for index,row in video_df.iterrows():
        try:
            # Extract hours, minutes, and seconds from the video duration string using regex
            hours_match = re.search(r'(\d+)H', row["Duration"])
            minutes_match = re.search(r'(\d+)M', row["Duration"])
            seconds_match = re.search(r'(\d+)S', row["Duration"])

            # Initialize time variables
            hours = 0
            minutes = 0
            seconds = 0
            
            if hours_match:
                hours = int(hours_match.group(1))
            if minutes_match:
                minutes = int(minutes_match.group(1))
            if seconds_match:
                seconds = int(seconds_match.group(1))

            # Format the duration as HH:MM:SS
            duration_formatted = "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)
        except:
            pass
        
        # Convert published date to a datetime object
        pub_datetime_string =str(row["Published_Date"])
        pub_dt_object = str(datetime.datetime.strptime(pub_datetime_string, '%Y-%m-%dT%H:%M:%SZ'))

        # Insert video data into the 'videos' table
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

        values=[str(row['Channel_Name']),
                str(row['Channel_Id']),
                str(row['Video_Id']),
                str(row['Title']),
                str(row['Tags']),
                str(row['Thumbnail']),
                str(row['Description']),
                pub_dt_object,
                duration_formatted,
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                str(row['Definition']),
                str(row['Caption_Status'])]
        cursor.execute(insert_query,values)
        mydb.commit()




# Create comments table in MySQL
def comments_table(single_ch_name1):

    # Create the 'comments' table if it doesn't exist
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(50),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published TIMESTAMP)'''
    cursor.execute(create_query)
    mydb.commit()

    # Convert MongoDB comments info document to DataFrame
    coll1=db["channel_details"]
    com_list=[]
    for com_data in coll1.find({"channel_information.Channel_Name":single_ch_name1},{"_id":0}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i]["data"])    

    comment_df=pd.DataFrame(com_list)

    # Insert comment data into the 'comments' table
    for index,row in comment_df.iterrows():
        datetime_string =str(row['Comment_Published'])
        dt_object = datetime.datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ')
        insert_query='''insert into comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published) values(%s,%s,%s,%s,%s)'''

        values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    dt_object
                    )
        
        cursor.execute(insert_query,values)
        mydb.commit()


# Main function to create all necessary tables for a channel
def tables(single_ch_name):
    
    # Call the functions to create tables and insert data
    news=channels_table(single_ch_name)
        
    if news: # If 'news' is not None(i.e., Channel already exists), return it as a message
        return news
    else:
        playlists_table(single_ch_name)
        videos_table(single_ch_name)
        comments_table(single_ch_name)

        return f"Channel {single_ch_name} migration successful."

# Fetch all channel data and return as a DataFrame
def show_channels_table():
    ch_list=[]
    for result in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(result["channel_information"])
    st_channel_df=st.dataframe(ch_list)
    
    return st_channel_df

# Fetch all playlist data and return as a DataFrame
def show_playlists_table():
    pl_list=[]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    st_playlist_df=st.dataframe(pl_list)
    
    return st_playlist_df

# Fetch all video data and return as a DataFrame
def show_videos_table():
    vi_list=[]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    st_video_df=st.dataframe(vi_list)

    return st_video_df

# Fetch all comment data and return as a DataFrame
def show_comments_table():
    com_list=[]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i]["data"])
    st_comment_df=st.dataframe(com_list)

    return st_comment_df



#Streamlit Part:

# Custom CSS Styling for the app
custom_css = """
<style>
    /* Sidebar Title & Main Title */
    h1 {
        color: #9fb1be !important;
    }
    /* Skill Take Away */
    h2 {
        color: white !important;
    }
    /* Set the color of the buttons to white text and teal background */
    .stButton>button {
        background-color: #1b72a1;
        color: black;
        padding: 10px 20px;
        border-radius: 5px;
        border: none;
        font-size: 16px;
        cursor: pointer;
    }
    .stButton>button:hover {
        background-color: #003366;
        color: white
    }
</style>
"""

# Applying the theme
st.markdown(custom_css, unsafe_allow_html=True)

with st.sidebar:
    st.title("YOUTUBE DATA HAVERSTING AND WAREHOUSING")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

st.title("Welcome to YouTube Data Harvesting")

# Button to get Channel ID & store it in MongoDB
channel_id=st.text_input("Enter the Channel ID")


if st.button("Collect and Store data"):
    ch_ids=[]
    coll1=db["channel_details"]

    # Check if the channel ID already exists
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)



# Dropdown for selecting the channel to migrate to MySQL
#all_channel=[]
#for result in coll1.find({},{"_id":0,"channel_information":1}):
#    all_channel.append(result["channel_information"]["Channel_Name"])
all_channel = [ch_data["channel_information"]["Channel_Name"] for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1})]
unique_name=st.selectbox("Choose the Channel to Migrate",all_channel)

if st.button("Migrate to SQL"):
    Table=tables(unique_name)
    st.success(Table)


show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#Questions Part:

# Function to check if required tables exist in the database
def check_tables_exists(cursor):
    cursor.execute("SHOW TABLES LIKE 'videos'")
    videos_exists = cursor.fetchone() is not None
    cursor.execute("SHOW TABLES LIKE 'channels'")
    channels_exists = cursor.fetchone() is not None
    cursor.execute("SHOW TABLES LIKE 'playlists'")
    playlists_exists = cursor.fetchone() is not None
    cursor.execute("SHOW TABLES LIKE 'comments'")
    comments_exists = cursor.fetchone() is not None
    return videos_exists and channels_exists and comments_exists and playlists_exists


# Check if the necessary tables exist
tables_exist = check_tables_exists(cursor)

if tables_exist:
    question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                                "2. Channels with most number of videos",
                                                "3. Top 10 most viewed videos",
                                                "4. No. of comments in each videos",
                                                "5. Videos with higest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))


    # Execute queries based on the user's selection
    if question=="1. All the videos and the channel name":
        query='''select Title,Channel_Name from videos'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Video Name','Channel Name'])
        st_result_df=st.write(result_df)

    elif question=="2. Channels with most number of videos":
        query='''select Channel_Name,Total_Videos from channels order by Total_Videos DESC'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Channel Name','No of Videos'])
        st_result_df=st.write(result_df)

    elif question=="3. Top 10 most viewed videos":
        query='''select Views,Title,Channel_Name from videos WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Views','Video Title','Channel Name'])
        st_result_df=st.write(result_df)

    elif question=="4. No. of comments in each videos":
        query='''SELECT Comments,Title FROM videos WHERE comments IS NOT NULL AND comments != 0'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['No of Comments','Video Title'])
        st_result_df=st.write(result_df)

    elif question=="5. Videos with higest likes":
        query='''SELECT Title,Likes,Channel_Name FROM videos WHERE Likes IS NOT NULL AND Likes != 0 ORDER BY Likes DESC'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Video Title',"Likes","Channel Name"])
        st_result_df=st.write(result_df)

    elif question=="6. Likes of all videos":
        query='''SELECT Title,Likes,Channel_Name FROM videos'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Video Title',"Likes","Channel Name"])
        st_result_df=st.write(result_df)

    elif question=="7. Views of each channel":
        query='''SELECT Channel_Name,Views FROM channels'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=["Channel Name","View Count"])
        st_result_df=st.write(result_df)

    elif question=="8. Videos published in the year of 2022":
        query='''SELECT Title,DATE(Published_Date),Channel_Name FROM videos WHERE EXTRACT(YEAR FROM Published_Date) = 2022 ORDER BY Published_Date'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=['Video Title',"Published Date","Channel Name"])
        st_result_df=st.write(result_df)

    elif question=="9. Average duration of all videos in each channel":
        query='''select Channel_Name,SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) as avgduration from videos GROUP BY Channel_Name'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=["channelname","avgduration"])
        result1=[]
        for index,row in result_df.iterrows():
            channel_title=row["channelname"]
            avg_duration=row["avgduration"]
            avg_dur_str=str(avg_duration)
            result1.append(dict(Channel_Name=channel_title,Average_Duration=avg_dur_str))
        result1_df=pd.DataFrame(result1)
        st_result_df=st.write(result1_df)

    elif question=="10. Videos with highest number of comments":
        query='''SELECT Comments,Title,Channel_Name FROM videos where Comments is not null and Comments !=0 ORDER BY Comments DESC'''
        cursor.execute(query)
        result=cursor.fetchall()
        result_df=pd.DataFrame(result,columns=["Comments",'Video Title',"Channel Name"])
        st_result_df=st.write(result_df)
else:
    # Show message if tables are not available
    st.info("Please migrate the tables to SQL before using this application.")
