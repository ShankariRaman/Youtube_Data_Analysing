import streamlit as st
import googleapiclient.discovery
import pymongo
import pymysql
import json
import pickle
import pandas as pd
import numpy as np
import re


# Constants
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
MONGODB_CONNECTION_STRING = "mongodb://localhost:27017"
SQL_CONNECTION_STRING = f"host={'localhost'};user={'root'};password={'Shankii007@'};database={'youtubedb'}"

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_client["youtube_data"]

# Connect to SQL database
sql_connection = pymysql.connect(host='localhost',user='root',password='Shankii007@',database='youtubedb')

# Initialize Google API client
youtube = googleapiclient.discovery.build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey="AIzaSyAUqexzmHlM3JEvoXPMMVJHC1eSQti6l-Y")

# Global variable for channel_data
channel_data = None

# Function to retrieve YouTube channel data
def get_channel_data(channel_id):
    global channel_data
    response = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    ).execute()

    if response["items"]:
        channel = response["items"][0]

        channel_data = {
            "channel_name": channel["snippet"]["title"],
            "channel_id": channel_id,
            "subscribers_count": channel["statistics"]["subscriberCount"],
            "channel_views": channel["statistics"]["viewCount"],
            "channel_description": channel["snippet"]["description"],
            "video_count": channel["statistics"]["videoCount"],
            "playlist_id": None,
            "playlist_name": None,
            'publishedAt': channel["snippet"]["publishedAt"],
            'total_videos': channel["statistics"]["videoCount"],
            "videos": []
        }


        # Get playlist ID
        playlists = youtube.playlists().list(
            part="id",
            channelId=channel_id,
            maxResults=3
        ).execute()


        if playlists["items"]:
            for ind in range(len(playlists["items"])):
                channel_data["playlist_id"] = playlists["items"][ind]["id"]
                channel_data["playlist_name"] = playlists["items"][ind]["id"]


            # Get videos from the playlist
                if channel_data["playlist_id"]:
                    playlist_items = youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        playlistId=channel_data["playlist_id"],
                        maxResults=50  # Adjust the number of videos to retrieve per request
                    ).execute()

                    for item in playlist_items["items"]:
                        video = item["snippet"]
                        video1 = item["contentDetails"]
                        video_data = {
                            "video_id": video["resourceId"]["videoId"],
                            "video_name": video["title"],
                            "description": video["description"],
                            "publishedAt": video["publishedAt"],
                            "duration": None,
                            #"caption_status": video["captionStatus"],
                            "view_count": None,
                            "comments": None,
                            "comment_count": None,
                            "likes": None,
                            "dislikes": None,
                            "favourites": None
                        }

                        # Get video statistics (likes, dislikes)
                        video_response = youtube.videos().list(
                            part="statistics,contentDetails",
                            id=video_data["video_id"]
                        ).execute()

                        if video_response["items"]:
                            statistics = video_response["items"][0]["statistics"]
                            statistics1 = video_response["items"][0]["contentDetails"]
                            video_data["likes"] = statistics.get("likeCount", 0)
                            video_data["dislikes"] = statistics.get("dislikeCount", 0)
                            video_data["favourites"] = statistics.get("favourites_count", 0)
                            video_data["view_count"] = statistics.get("viewCount", 0)
                            video_data["comment_count"] = statistics.get("commentCount", 0)
                            video_data["duration"] = statistics1.get("duration", 0)
                            video_data["duration"] = video_data["duration"] if video_data["duration"] != None else 'PT1S'
                            try:
                                parsed_duration = re.search(f"PT(\d+H)?(\d+M)?(\d+S)", video_data["duration"]).groups()
                                duration_str = ""
                                for d in parsed_duration:
                                    if d:
                                        duration_str += f"{d[:-1]}:"
                                video_data["duration"]= duration_str.strip(":")
                                video_data["duration"]=video_data["duration"].split(':')
                                coun = 0
                                #print(f'shan{video_data["duration"]}')
                                if len(video_data["duration"]) == 3:
                                    coun +=int(video_data["duration"][0]) * 3600
                                    coun +=int(video_data["duration"][1]) * 60
                                    coun +=int(video_data["duration"][2])
                                    video_data["duration"]= coun
                                elif len(video_data["duration"]) == 2:
                                    coun += int(video_data["duration"][0]) * 60
                                    coun += int(video_data["duration"][1])
                                    video_data["duration"] = coun
                                elif len(video_data["duration"]) == 1:
                                    coun += int(video_data["duration"][0])
                                    video_data["duration"] = coun
                                else:
                                    coun = 12
                                    video_data["duration"] = coun
                            except:
                                video_data["duration"]= 12


                        # Get video comments
                        comment_response = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_data["video_id"],
                            maxResults=50 # Adjust the number of comments to retrieve per request
                        ).execute()

                        video_data["comments"] = [comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                                                  for comment in comment_response["items"]]

                        channel_data["videos"].append(video_data)

        return channel_data

    return None
# Function to store channel data in MongoDB
def store_channel_data_in_mongodb(channel_data):
    collection = mongo_db["youtube_channels"]
    collection.delete_many({'channel_name': channel_data["channel_name"]})

    collection.insert_one(channel_data)




# Function to migrate channel data from MongoDB to SQL database
def migrate_channel_data_to_sql(channel_name):

    collection = mongo_db["youtube_channels"]
    channel_data = collection.find_one({"channel_name": channel_name})

    if True:
        with sql_connection.cursor() as cursor:


            cursor.execute("""
                            CREATE TABLE IF NOT EXISTS {} (
                                channel_name VARCHAR(255) ,
                                channel_id VARCHAR(255),
                                video_count INT,
                                total_views INT,
                                video_name VARCHAR(255),
                                publishedAt TIMESTAMP,
                                duration INT,
                                view_count INT,
                                likes INT,
                                dislikes INT,
                                comment_count INT
                                )
                        """.format('channelnew'.replace(" ", "_")))
            cursor.execute(f"""delete from channelnew where channel_name ='{channel_data["channel_name"]}' """)

            # Insert video data into the channel table
            for video in channel_data["videos"]:

                cursor.execute(f"""
                               INSERT INTO  channelnew
                               VALUES ('{channel_data["channel_name"]}','{channel_data["channel_id"]}',{channel_data["video_count"]},{channel_data["channel_views"]},'{video["video_name"]}','{pd.to_datetime(video["publishedAt"]).strftime("%Y-%m-%d")}',{video["duration"]},{video["view_count"]},{video["likes"]},{video["dislikes"]},{video["comment_count"]})
                           """)

        sql_connection.commit()



# Function to search and retrieve data from SQL database
def search_data_in_sql(vir):
    quer = ["select video_name,channel_name from channelnew limit 50",
            "select channel_name,video_count from channelnew where video_count=(select max(video_count) from channelnew )limit 1",
            "select channel_name,video_name,view_count from channelnew order by view_count desc limit 10",
            "select video_name,comment_count from channelnew limit 50",
            "select video_name,likes,channel_name from channelnew where likes=(select max(likes) from channelnew )limit 1",
            "select likes,dislikes,video_name from channelnew limit 50",
            "select channel_name,avg(total_views) as TotalViews from channelnew group by channel_name limit 50",
            "select distinct(channel_name) from channelnew where publishedAt > '2021-12-31' and publishedAt < '2023-01-01' ",
            "select avg(duration),channel_name from channelnew group by channel_name",
            "select video_name,comment_count,channel_name from channelnew where comment_count=(select max(comment_count) from channelnew)limit 1"]
    results = pd.read_sql_query(f"""{quer[int(vir) - 1]}""", sql_connection)
    #print(results)
    return results





# Streamlit app
def main():
    global channel_data1
    global channel_data

    st.title("YouTube Data Analyzer")

    # Sidebar
    st.sidebar.title("Options")

    # Option 1: Retrieve YouTube channel data
    st.sidebar.header("Retrieve Channel Data")
    channel_id = st.sidebar.text_input("Enter Channel ID")
    if st.sidebar.button("Retrieve Data"):
        channel_data = get_channel_data(channel_id)
        pickle.dump(channel_data, open("channel_data.pkl", "wb"))
        ###print(type(channel_data))

        if channel_data:
            st.sidebar.success("Data retrieved successfully!")
            #st.sidebar.json(channel_data)
        else:
            st.sidebar.error("Channel data not found.")

    # Option 2: Store data in MongoDB
    st.sidebar.header("Store Data in MongoDB")
    if st.sidebar.button("Store Data"): #and channel_data:
        channel_data = pickle.load(open("channel_data.pkl",'rb'))
        ###print("shan " +str(type(channel_data)))
        store_channel_data_in_mongodb(channel_data)
        st.sidebar.success("Data stored in MongoDB!")

    # Option 3: Migrate data to SQL
    st.sidebar.header("Migrate Data to SQL")
    temp_collection = mongo_db["youtube_channels"]
    m_list =temp_collection.distinct('channel_name')
    try:
        s_list = pd.read_sql_query(f"""select distinct(channel_name) as dis from channelnew""", sql_connection)['dis'].to_list()
    except Exception as E: 
        print(E)
        s_list=[]
    n_list = []
    for i in m_list:
        if i in s_list:
            n_list.append('(M)'+i)
        else:
            n_list.append(i)
    st.sidebar.write("(M) Already Migrated")
    channel_name = st.sidebar.selectbox("Select Channel Name", n_list)
    if channel_name :
        if channel_name[0] == '(':
            channel_name=channel_name[3:]
    if channel_name :
        pickle.dump(channel_name, open("channel_name.pkl", "wb"))
    if st.sidebar.button("Migrate Data"): #and channel_data:
        channel_name = pickle.load(open("channel_name.pkl", 'rb'))
        migrate_channel_data_to_sql(channel_name)
        st.sidebar.success("Data migrated to SQL!")

    # Option 4: Search data in SQL
    st.sidebar.header("Search Data in SQL")
    search_channel_name = st.sidebar.selectbox("Select Query", ['1 What are the names of all the videos and their corresponding channels?','2 Which channels have the most number of videos, and how many videos do they have?','3 What are the top 10 most viewed videos and their respective channels?','4 How many comments were made on each video, and what are their corresponding video names?','5 Which videos have the highest number of likes, and what are their corresponding channel names?','6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?','7 What is the total number of views for each channel, and what are their corresponding channel names?','8 What are the names of all the channels that have published videos in the year 2022?','9 What is the average duration of all videos in each channel, and what are their corresponding channel names?','10 Which videos have the highest number of comments, and what are their corresponding channel names?'] )
    #search_channel_name=channel_name
    if st.sidebar.button("Search Data"):


        if search_channel_name[:2]== '10':
            search_channel_name=search_channel_name.strip()[:2]
        else:
            search_channel_name=search_channel_name.strip()[0]
        search_results = search_data_in_sql(search_channel_name)

        if True:
            st.sidebar.success("Search results:")
            st.table(search_results)

        else:
            st.error("No results found.")


    # Main content
    st.subheader("YouTube Channel Data")
    if channel_data:
        st.json(channel_data)


if __name__ == "__main__":
    main()
