
import streamlit as st
import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.discovery import build
import pymongo
import mysql.connector
from mysql.connector.errors import  Error
import isodate
from sqlalchemy import create_engine

clnt = pymongo.MongoClient("mongodb://localhost:27017")
mdb = clnt['Ytextract']
mcol = mdb['YTChanneldata']
engine = create_engine('mysql+mysqlconnector://root:12345@localhost:3306/Ytextract')
api_service_name = "youtube"  # this specifies the service "YouTube" from Google api client
api_version = "v3"  # specifies the version of API
api_key = "AIzaSyDo5WNdLbB2tRmtZN-pbDYFCQJqFRstwuY"  # providing the API credentials
youtube = googleapiclient.discovery.build(
    # creating a service object to interact with Google API and storing it in a variable "YouTube"
    api_service_name,  # provide the necessary arguments for the build function
    api_version,
    developerKey=api_key)


def get_channeldata(Channelid):
    api_service_name = "youtube"  # this specifies the service "YouTube" from Google api client
    api_version = "v3"  # specifies the version of API
    api_key = "AIzaSyDo5WNdLbB2tRmtZN-pbDYFCQJqFRstwuY"  # providing the API credentials
    youtube = googleapiclient.discovery.build(
        # creating a service object to interact with Google API and storing it in a variable "youtube"
        api_service_name,  # provide the necesary arguments for the build function
        api_version,
        developerKey=api_key)
    response = youtube.channels().list(  # Using channels().list()method to retrive list of channels
        # data.
        part='snippet,statistics,contentDetails',  # retrieving only snippet , stats,contentdetails parts of
        # the API data
        id=Channelid,  # filtering by ID which is the channel Id stored in the
        # channel_id variable
        fields="items(snippet(title),id,statistics(subscriberCount),statistics(viewCount),snippet(description),contentDetails(relatedPlaylists(uploads)))"
    ).execute()
    channel_ID = response['items'][0]['id']  # Accessing id data and storing in Channel_ID
    channel_snippet = response['items'][0]['snippet']  # Accessing Snippet data and storing in Channel_snippet
    channel_statistics = response['items'][0]['statistics']  # accessing Stats and storing in channe_statistics
    channel_content = response['items'][0]['contentDetails']
    data = {
        'Channel_Name': channel_snippet['title'],
        'Channel_Id': channel_ID,
        'Subscription_Count': int(channel_statistics['subscriberCount']),
        'Channel_Views': int(channel_statistics['viewCount']),
        'Channel_Description': channel_snippet['description'],
        'Playlist_Id': channel_content['relatedPlaylists']['uploads']
    }
    return data


def get_playlist_data(ChannelID):
    response = youtube.channels().list(  # Using channels().list()method to retrive list of channels data.
        part='snippet,contentDetails',  # retrievig only snippet , stats,contentdetails parts of the API data
        id=ChannelID,  # filtering by ID which is the channel Id stored in the channel_id variable
        fields="items(snippet(title),id,contentDetails(relatedPlaylists(uploads)))"
    ).execute()
    channel_ID = response['items'][0]['id']  # Accessing id data and storing in Channel_ID
    channel_snippet = response['items'][0]['snippet']  # Accessing Snippet data and storing in Channel_snippet
    channel_content = response['items'][0]['contentDetails']
    data = {
        'Playlist_Id': channel_content['relatedPlaylists']['uploads'],
        'Channel_Id': channel_ID,
        'Playlist_Name': channel_snippet['title']
    }
    return data


def get_video_list(ChannelID):
    response = youtube.playlistItems().list(  # Using playlistItems().list()method to retrive list of playlist data.
        part="contentDetails",  # retrieving only contentdetails part
        playlistId=get_channeldata(ChannelID)['Playlist_Id'],
        maxResults=50).execute()  # using execute method to retrieve data and store it in "response"
    videolist = []  # defining an empty list "videolist"
    for item in response['items']:  # looping for all items in the response and adding each data into the list
        videolist.append(item['contentDetails']['videoId'])

    next_page_token = response.get(
        "nextPageToken")  # getting the nextpage token and storing in the variable next_page_token
    while next_page_token is not None:  # using while loop to iterate over the service object to keep getting all pages of data
        response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=get_channeldata(ChannelID)['Playlist_Id'],
            pageToken=next_page_token,
            maxResults=50).execute()

        for item in response['items']:  # appending all data into list
            videolist.append(item['contentDetails']['videoId'])
        next_page_token = response.get("nextPageToken")
    return videolist


def get_video_data(ChannelID):
    videodata = {}
    sn = 1
    videolist = get_video_list(ChannelID)
    for i in videolist:
        video_resp = youtube.videos().list(
            part='id,snippet,statistics,contentDetails',
            id=i,
            fields='items(id,snippet.title,snippet.description,snippet.thumbnails.default.url,snippet.tags,snippet.publishedAt,statistics.viewCount,statistics.likeCount,statistics.dislikeCount,statistics.favoriteCount,statistics.commentCount,contentDetails.duration,contentDetails.caption)'
        ).execute()
        Video_ID = video_resp['items'][0]['id']
        Video_snippet = video_resp['items'][0]['snippet']
        Video_statistics = video_resp['items'][0]['statistics']
        Video_contentDetails = video_resp['items'][0]['contentDetails']
        data = {
            "Video_Id": Video_ID,
            "Playlist_Id": get_playlist_data(ChannelID)['Playlist_Id'],
            "Video_Name": Video_snippet['title'].replace("'", "\\'"),
            "Video_Description": Video_snippet['description'].replace("'", "\\'"),
            "Tags": Video_snippet['tags'] if 'tags' in Video_snippet else 'NA',
            "PublishedAt": Video_snippet['publishedAt'],
            "View_Count": int(Video_statistics['viewCount']),
            "Like_Count": int(Video_statistics['likeCount']),
            "Favorite_Count": int(Video_statistics['favoriteCount']),
            "Comment_Count": int(Video_statistics['commentCount']),
            "Duration": Video_contentDetails['duration'],
            "Thumbnail": Video_snippet['thumbnails']['default']['url'],
            "Caption_Status": 'Available' if Video_contentDetails['caption'] is True else 'Not Available'
        }
        data = {'videoid_' + str(sn): data}
        sn += 1
        videodata.update(data)
    return videodata


def get_comment_data(ChannelID):
    Commentdata = {}
    sn = 1
    videolist = get_video_list(ChannelID)
    for i in videolist:
        comment_resp = youtube.commentThreads().list(
            part="snippet,id",
            videoId=i,
        ).execute()
        next_page_token = comment_resp.get("nextPageToken")

        for item in comment_resp['items']:
            comment_top = comment_resp['items'][comment_resp['items'].index(item)]['snippet']
            commentcol = {
                "Comment_Id": comment_top['topLevelComment']['id'],
                "video_Id": comment_top['videoId'],
                "Comment_Text": comment_top['topLevelComment']['snippet']['textDisplay'],
                "Comment_Author": comment_top['topLevelComment']['snippet']['authorDisplayName'],
                "Comment_PublishedAt": comment_top['topLevelComment']['snippet']['publishedAt']
            }
            data = {'Comment_' + str(sn): commentcol}
            sn += 1
            Commentdata.update(data)
        next_page_token = comment_resp.get("nextPageToken")
        while next_page_token is not None:
            comment_resp = youtube.commentThreads().list(
                part="snippet,id",
                videoId=i,
                pageToken=next_page_token
            ).execute()
            for item in comment_resp['items']:
                comment_top = comment_resp['items'][comment_resp['items'].index(item)]['snippet']
                commentcol = {
                    "Comment_Id": comment_top['topLevelComment']['id'],
                    "video_Id": comment_top['videoId'],
                    "Comment_Text": comment_top['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": comment_top['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": comment_top['topLevelComment']['snippet']['publishedAt']

                }
                data = {'Comment_' + str(sn): commentcol}
                sn += 1
                Commentdata.update(data)
            next_page_token = comment_resp.get("nextPageToken")
    return Commentdata


def main(ChannelID):
    channeldata = {'channeldata': get_channeldata(ChannelID), 'playlist_data': get_playlist_data(ChannelID),
                   'videodata': get_video_data(ChannelID), 'commentdata': get_comment_data(ChannelID)}
    return channeldata


def createtables():
    myconnection = mysql.connector.connect(host='localhost', user='root', password='12345',
                                           auth_plugin='mysql_native_password')
    cursor = myconnection.cursor()
    cursor.execute("Use Ytextract")

    cursor.execute('''create table channel (
                      Channel_Id Varchar(255) primary key,
                      Channel_Name varchar(255),
                      Subscription_Count int,
                      Channel_Views int,
                      Channel_Description text,
                      Playlist_Id varchar(255)
                      )
                      ''')
    print("channel created")
    cursor.execute('''create table playlist(
                              Playlist_Id varchar(255) primary key,
                              Channel_Id varchar(255),
                              Playlist_Name varchar(255),
                              CONSTRAINT fk_playlist
                              FOREIGN KEY(Channel_Id)
                              REFERENCES channel(Channel_Id))
                           ''')
    print("playlist cretaed")
    cursor.execute("""create table video (
                              Video_Id varchar(255) primary key,
                              playlist_id varchar(255),
                              Video_Name varchar(255),
                              Video_Description text,
                              PublishedAt datetime,
                              View_Count int,
                              Like_Count int,
                              Favorite_Count int,
                              Comment_Count int,
                              Duration int,
                              Thumbnail varchar(255),
                              Caption_Status varchar(255),
                              CONSTRAINT fk_video
                              FOREIGN KEY(playlist_id)
                              REFERENCES playlist(Playlist_Id)
                                )
                                  """)
    print("video created")

    cursor.execute('''create table comment(
                              Comment_Id varchar(255) primary key,
                              video_Id varchar(255),
                              Comment_Text text,
                              Comment_Author varchar(255),
                              Comment_PublishedAt datetime,
                              CONSTRAINT fk_comment
                              FOREIGN KEY(video_Id)
                              REFERENCES video(Video_Id)
                                               )
                        ''')


def extract(ChannelID):
    channeldoc = main(ChannelID)
    mcol.replace_one({'channeldata.Channel_Id': ChannelID}, channeldoc, upsert=True)


def migration(channellist):
    myconnection = mysql.connector.connect(host='localhost', user='root', password='12345',
                                           auth_plugin='mysql_native_password')
    cursor = myconnection.cursor()
    print("connected")
    engine = create_engine(
        'mysql+mysqlconnector://root:12345@localhost:3306/Ytextract?auth_plugin=mysql_native_password')
    try:
        cursor.execute("Create database Ytextract")
        myconnection.commit()
        print("db create")

    except DatabaseError:
        cursor.execute("Use Ytextract")
        print("use ytextract")

    try:
        cursor.execute('drop table comment,video,playlist,channel')
        createtables()
    except Error:
        createtables()
    Channeldf = pd.DataFrame()
    playlistdf = pd.DataFrame()
    videodf = pd.DataFrame()
    commentdf = pd.DataFrame()

    for i in channellist:
        docs = mcol.find_one({'channeldata.Channel_Name': i})
        chdf = pd.DataFrame(docs['channeldata'], index=[0])
        pldf = pd.DataFrame(docs['playlist_data'], index=[0])
        vidf = pd.DataFrame(docs['videodata']).transpose().drop('Tags', axis=1)
        vidf['PublishedAt'] = vidf['PublishedAt'].str.replace('T', ' ').str.replace('Z', '')
        vidf['Duration'] = vidf['Duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())
        cmdf = pd.DataFrame(docs['commentdata']).transpose()
        cmdf['Comment_PublishedAt'] = cmdf['Comment_PublishedAt'].str.replace('T', ' ').str.replace('Z', '')
        Channeldf = pd.concat([Channeldf, chdf], ignore_index=True)
        playlistdf = pd.concat([playlistdf, pldf], ignore_index=True)
        videodf = pd.concat([videodf, vidf], ignore_index=True)
        commentdf = pd.concat([commentdf, cmdf], ignore_index=True)
    Channeldf.to_sql('channel', con=engine, if_exists='append', index=False)
    playlistdf.to_sql('playlist', con=engine, if_exists='append', index=False)
    videodf.to_sql('video', con=engine, if_exists='append', index=False)
    commentdf.to_sql('comment', con=engine, if_exists='append', index=False)


st.title('YouTube Channel Data')
with st.form('channel_id_form'):
    channel_id = st.text_input('Enter a YouTube channel ID:')
    submit_button = st.form_submit_button('Search')
if submit_button:
    with st.spinner('Extracting data...'):
        extract(channel_id)
    st.success('Data Successfully Extracted!')
dropdown = [doc['channeldata']['Channel_Name'] for doc in mcol.find({})]
with st.form('migrationform'):
    export_button = st.form_submit_button('Export')

channellist = st.multiselect('select the channels to export to sql', dropdown, 'Data Science Analytics')
print(channellist)
if export_button:
    try:
        migration(channellist)
        st.success('Data Successfully Exported!')
    except Error as err:
        print(err)
import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector.errors import DatabaseError, Error

myconnection = mysql.connector.connect(host='localhost', user='root', password='12345',
                                       auth_plugin='mysql_native_password')
cursor = myconnection.cursor()


def runquery(your_query):
    cursor.execute('use ytextract')
    sqlquery = querymap[your_query]
    cursor.execute(sqlquery)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=column_names)
    df.index = df.index + 1
    return df


dropdown = ["1.What are the names of all the videos and their corresponding channels?",
            "2.	Which channels have the most number of videos, and how many videos do they have?",
            "3.	What are the top 10 most viewed videos and their respective channels?",
            "4.	How many comments were made on each video, and what are their corresponding video names?",
            "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
            "8.	What are the names of all the channels that have published videos in the year 2022?",
            "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"]
queries = st.multiselect('select your query', dropdown)
querymap = {"1.What are the names of all the videos and their corresponding channels?": '''SELECT video.Video_Name, channel.Channel_Name 
                  FROM video JOIN channel  ON video.Playlist_Id = channel.Playlist_Id''',
            "2.	Which channels have the most number of videos, and how many videos do they have?": '''SELECT channel.Channel_Name, COUNT(video.Video_Id) AS Video_Count 
                  FROM channel JOIN video  ON channel.Playlist_Id = video.Playlist_Id
                  GROUP BY channel.Channel_Name
                  ORDER BY Video_Count DESC''',
            "3.	What are the top 10 most viewed videos and their respective channels?": '''Select video.Video_Name,channel.channel_name,video.View_Count 
                  From channel join video on video.Playlist_Id=channel.Playlist_Id
                  Order by video.View_Count DESC
                  LIMIT 10''',
            "4.	How many comments were made on each video, and what are their corresponding video names?": '''Select video.Video_Name,count(comment.comment_id) as commentcount 
                  From video join comment on video.video_Id=comment.video_Id
                  group by  video.Video_Name''',
            "5.	Which videos have the highest number of likes, and what are their corresponding channel names?": '''Select video.video_name,Channel.channel_name,video.like_count
                  from video join channel on video.playlist_id=channel.playlist_id
                  order by video.like_count Desc
                  limit 10''',
            "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?": '''Select video.video_name,video.like_count from video''',
            "7.	What is the total number of views for each channel, and what are their corresponding channel names?": '''select channel.channel_name,Channel.channel_views from channel''',
            "8.	What are the names of all the channels that have published videos in the year 2022?": '''SELECT Distinct channel.channel_name
                  FROM video join channel on video.Playlist_Id=channel.Playlist_Id
                  WHERE YEAR(PublishedAt) = 2022''',
            "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?": '''select channel.channel_name, avg(video.duration) as duration
                  from channel join video on channel.playlist_id=video.playlist_id
                  group by channel.channel_name''',
            "10.Which videos have the highest number of comments, and what are their corresponding channel names?": '''SELECT video.Video_Name, channel.Channel_Name, COUNT(comment.Comment_Id) AS commentcount
                  FROM video
                  JOIN comment ON video.Video_Id = comment.Video_Id
                  JOIN channel ON video.Playlist_Id = channel.Playlist_Id
                  GROUP BY video.Video_Name, channel.Channel_Name
                  ORDER BY commentcount DESC
                  limit 10'''}
print(queries)
for query in queries:
    st.dataframe(runquery(query))

# In[ ]:
