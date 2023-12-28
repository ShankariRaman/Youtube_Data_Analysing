###--- install and import required packages ---
import googleapiclient.discovery
from googleapiclient.discovery import build
from datetime import datetime as dt
from isodate import parse_duration
import pandas as pd
import streamlit as st

### ---mongodb connection---
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import urllib
encoded_password = urllib.parse.quote_plus('Shankii007') #password should not contain any symbol
uri = f"mongodb+srv://shankariraman104:{encoded_password}@cluster0.pv5cuo5.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

### ---sql connection---
import sqlite3
    
# ---setting API name,version, key---

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyC1257kNgVMtJGmlYmlbQ8bkaziRg6xYdc"

youtube = build(api_service_name, api_version, developerKey=api_key)

# ---get channel data---

channel_informations = None 

def get_channel_data(channel_id): #initializing function called get_channel_data
  global channel_informations
  request = youtube.channels().list(              #request to get the channel data
        part="snippet,contentDetails,statistics",
        id= channel_id
    )
  channel_response = request.execute()

  if channel_response['items']:
    channel_informations = {         #channel_informations --> to collect the channel details
        'channel_id': channel_id,
        'channel_name' : channel_response['items'][0]['snippet']['title'],
        'channel_description' : channel_response['items'][0]['snippet']['description'],
        'Channel_PublishedAt': dt.strptime((channel_response['items'][0]['snippet']['publishedAt']),'%Y-%m-%dT%H:%M:%SZ'),
        'Total_subscribers':channel_response['items'][0]['statistics']['subscriberCount'],
        'Total_videos':channel_response['items'][0]['statistics']['videoCount'],
        'Total_views': channel_response['items'][0]['statistics']['viewCount'],
        'Playlists': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'Videos':[]}

# from playlist id get each videoid        
  request = youtube.playlistItems().list(           #request to get the playlist data
        part="snippet,contentDetails",
        maxResults=25,
        playlistId= channel_informations['Playlists']
    )
  playlist_response = request.execute()
  playlist_videos = playlist_response['items']

  for item in playlist_videos:
    video_id = item['snippet']['resourceId']['videoId']
   
    # after getting videoid we have to fetch video data
    request = youtube.videos().list(                  #request to get the video data      
        part="snippet,contentDetails,statistics",
        id=video_id
        )
    video_response = request.execute()
    if video_response['items']:
                video_informations = {          #video_informations --> to collect all the video info in that channel
                    "Video_Id": video_id,
                    "Video_Name": video_response['items'][0]['snippet']['title'] if 'title' in video_response['items'][0]['snippet'] else "Not Available",
                    "Video_Description": video_response['items'][0]['snippet']['description'],
                    'Total_Views':video_response['items'][0]['statistics']['viewCount'],
                    'Total_Likes':video_response['items'][0]['statistics']['likeCount'],
                    'Total_Dislikes':video_response['items'][0]['statistics'].get('dislikeCount',0),
                    'Total_Comments':video_response['items'][0]['statistics']['commentCount'],
                    "Video_PublishedAt":dt.strptime((video_response['items'][0]['snippet']['publishedAt']),'%Y-%m-%dT%H:%M:%SZ'),
                    "Video_Duration": parse_duration(video_response['items'][0]['contentDetails']['duration']).total_seconds(),
                    'Comments': {}
                    }
                
                # fetch the comment data from each video
                request = youtube.commentThreads().list(   #request to get the comment data
                  part="snippet,replies",
                  videoId=video_id,
                  maxResults = 4
                  )
                comment_response = request.execute()
                for comment_item in comment_response.get('items', []):

                  comment_informations = {   #comment_informations 
                                "Comment_Id": comment_item['snippet']['topLevelComment']['id'],
                                "Comment_Text": comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                "Comment_Author": comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                "Comment_PublishedAt":dt.strptime((comment_item['snippet']['topLevelComment']['snippet']['publishedAt']),'%Y-%m-%dT%H:%M:%SZ')
                               }

                  video_informations['Comments'][comment_informations['Comment_Id']] = comment_informations

                channel_informations['Videos'].append(video_informations)
  return channel_informations
  return None

### ---insert data into mongodb---

def insert_data_in_mongodb(channel_informations): #initiating function called insert_data_in_mongodb with parameter channel_informations
  database = client['Youtube_Data']   #creating database in mongodb called  Youtube_Data
  collection = database['Channel_Data'] #creating one collection in that database called Channel_Data
  collection.delete_many({'channel_name':channel_informations['channel_name']}) #to delete if the channel is already stored in mongodb using channel_name
  collection.insert_one(channel_informations) #or it will insert that data into mongodb

### ---insert data into sql 
  
def insert_data_in_sql(channel_informations):  #initiating function
    conn = sqlite3.connect('youtube_data_coll.db')  # creating database named youtube_data_coll
    cursor = conn.cursor()

    # ---Create Channel table---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Channels (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            channel_description TEXT,
            Channel_PublishedAt DATETIME,
            Total_subscribers INTEGER,
            Total_videos INTEGER,
            Total_views INTEGER,
            Playlists TEXT
        )
    ''')

    # ---Create Video table---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Videos (
        Video_id VARCHAR PRIMARY KEY,
        Channel_id VARCHAR,
        Channel_name VARCHAR,
        Video_name VARCHAR,
        Video_description TEXT,
        Total_views INT,
        Total_likes INT,
        Total_dislikes INT,
        Total_comments INT,
        Video_PublishedAt DATETIME,
        Video_Duration_InSeconds INT,
        FOREIGN KEY (Channel_id) REFERENCES Channels(Channel_id),
        FOREIGN KEY (Channel_name) REFERENCES Channels(Channel_name)
        )
    ''')

    # ---Create Comment table---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Comments (
        Comment_id VARCHAR PRIMARY KEY,
        Video_id VARCHAR,
        Comment_text VARCHAR,
        Comment_author VARCHAR,
        Comment_publishedAt DATETIME,
        FOREIGN KEY (Video_id) REFERENCES Videos(Video_id)
        )
    ''')

    try:
        # ---Insert or replace data into Channel table----
        cursor.execute('''
            INSERT OR REPLACE INTO Channels (
                channel_id, channel_name, channel_description, Channel_PublishedAt,
                Total_subscribers, Total_videos, Total_views, Playlists
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            channel_informations['channel_id'], channel_informations['channel_name'],
            channel_informations['channel_description'], channel_informations['Channel_PublishedAt'],
            channel_informations['Total_subscribers'], channel_informations['Total_videos'],
            channel_informations['Total_views'], channel_informations['Playlists']
        ))

        # ---Insert data into Video table---
        for video_info in channel_informations['Videos']:
            cursor.execute('''
                INSERT OR REPLACE INTO Videos (
                    Video_Id,channel_id, channel_name, Video_Name, Video_Description, Total_Views,
                    Total_Likes, Total_Dislikes, Total_Comments, Video_PublishedAt,
                    Video_Duration_InSeconds
                ) VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video_info['Video_Id'], channel_informations['channel_id'], channel_informations['channel_name'],
                video_info['Video_Name'], video_info['Video_Description'],
                video_info['Total_Views'], video_info['Total_Likes'], video_info['Total_Dislikes'],
                video_info['Total_Comments'], video_info['Video_PublishedAt'],
                video_info['Video_Duration']
            ))

            # ---Insert data into Comment table---
            for comment_info in video_info['Comments'].values():
                cursor.execute('''
                    INSERT OR REPLACE INTO Comments (
                        Comment_Id, Video_Id,Comment_Text, Comment_Author, Comment_PublishedAt
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    comment_info['Comment_Id'],video_info['Video_Id'], comment_info['Comment_Text'],
                    comment_info['Comment_Author'], comment_info['Comment_PublishedAt']

                ))

    except Exception as e:
        print(f"Error inserting data into SQLite: {e}")

    conn.commit()
    #conn.close()
insert_data_in_sql(channel_informations)

#---streamlit app--
import streamlit as st

st.set_page_config(           #setting page config
        page_title="Youtube Data Analyser",
        page_icon="youtube.png",
        layout = 'wide',

    )

def main():
    global channel_informations
    
    st.title("YouTube Data Harvesting & Warehousing")
    st.sidebar.header("Options")
    # Option 1: Retrieve YouTube channel data
    st.sidebar.subheader("Retrieve Channel Data")
    channel_id = st.sidebar.text_input("Enter Channel ID")
    if st.sidebar.button("Collect Data"):
        channel_informations = get_channel_data(channel_id)
        st.header('channel_details')
        st.write(channel_informations)
        if channel_informations:
            st.sidebar.success("Data retrieved successfully!")
        else:
            st.sidebar.error("Channel data not found.")
    
    # Option 2: Store channel data in Mongodb
    st.sidebar.subheader("Store Data in Mongodb")
    if st.sidebar.button("Store Data"):
        channel_informations = get_channel_data(channel_id)
        insert_data_in_mongodb(channel_informations)
        st.sidebar.success("Data stored successfully!")
    
    # Option 3: Migrate channel data from Mongodb to sql
    conn = sqlite3.connect('youtube_data_coll.db')  # Replace 'youtube_data.db' with your database name
    cursor = conn.cursor()
    st.sidebar.subheader("Migrate Data to SQL")
    if st.sidebar.button("Migrate Data"):
        channel_informations = get_channel_data(channel_id)
        insert_data_in_sql(channel_informations)
        st.sidebar.success("Data migrated successfully!")
    
    #option 4: Search query
    st.sidebar.subheader("Search Data in SQL")
    questions =  [ 
             '1. What are the names of all the videos and their corresponding channels?',
             '2. Which channels have the most number of videos, and how many videos do they have?',
             '3. What are the top 10 most viewed videos and their respective channels?',
             '4. How many comments were made on each video, and what are their corresponding video names?',
             '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
             '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
             '7. What is the total number of views for each channel, and what are their corresponding channel names?',
             '8. What are the names of all the channels that have published videos in the year 2023?',
             '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
             '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
             ]
    
    selected_question = st.sidebar.selectbox('Select Query', questions)
    if st.sidebar.button("Search Data"):
      st.write(f"Question: {selected_question}")
      if selected_question == questions[0]:
       result_query = cursor.execute('SELECT channel_name, video_name FROM Videos')
       result_df =pd.DataFrame(result_query.fetchall(), columns = ['Channel_name','Video_name'])
      elif selected_question == questions[1]:
       result_query = cursor.execute('SELECT channel_name, max(total_videos) from Channels ')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_count'])
      elif selected_question == questions[2]:
       result_query = cursor.execute('SELECT channel_name, video_name,total_views from Videos ORDER by total_views DESC LIMIT 10')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_name','View_count'])
      elif selected_question == questions[3]:
       result_query = cursor.execute('SELECT video_name, total_comments from Videos')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Video_name','Comment_count'])
      elif selected_question == questions[4]:
       result_query = cursor.execute('SELECT channel_name,video_name,max(total_likes) from Videos')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_name','Like_count'])
      elif selected_question == questions[5]:
       result_query = cursor.execute('SELECT channel_name, video_name, total_likes, total_dislikes from videos')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_name','Like_count','Dislike_count'])
      elif selected_question == questions[6]:
       result_query = cursor.execute('SELECT channel_name,total_views from Channels')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','total_views_count'])
      elif selected_question == questions[7]:
       result_query = cursor.execute("SELECT Channels.channel_name, Videos.video_name FROM videos JOIN Channels ON Videos.channel_id = Channels.channel_id WHERE strftime('%Y', video_publishedat) = '2023'")
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_name'])
      elif selected_question == questions[8]:
       result_query = cursor.execute('SELECT channel_name, avg(video_duration_inseconds)  from Videos group by channel_name')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Avg_dur_inseconds'])
      elif selected_question == questions[9]:
       result_query = cursor.execute('SELECT channel_name,video_name, max(total_comments) FROM Videos')
       result_df = pd.DataFrame(result_query.fetchall(),columns = ['Channel_name','Video_name','total_comment_count'])
       
      st.write(result_df)   
main()
