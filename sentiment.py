# -*- coding: utf-8 -*-

# importing packages
import praw
import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import datetime
import mysql.connector
from sqlalchemy import create_engine
np.set_printoptions(suppress=True) #suppress scientific notation
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# configuring connection to reddit
reddit = praw.Reddit(client_id=''
                    ,client_secret=''
                    ,password=''
                    ,user_agent=''
                    ,username='')

engine = create_engine('mysql+mysqlconnector://', echo=False)

df = pd.read_sql( '''select distinct Subreddit_Name
                     from MR ''',engine)
print (df.head())
print (df.tail())

query =  df['Subreddit_Name'].unique()
subreddits = []
 
# appending list with unique subreddit names
for name in query :
    subreddits.append(name)


# variables and lists
date = datetime.datetime.now()
analyzer = SentimentIntensityAnalyzer()
name_list = []
type_list = []
ups_list = []
downs_list = []
comment_date_list = []
pos_list = []
neg_list = []
neu_list = []
comp_list = []

#subreddit = reddit.subreddit('test')
for i in subreddits:
    try:
        subreddit = reddit.subreddit(i)
        print (subreddit)
        hot_python = subreddit.hot(limit=10)
        for submission in hot_python:
            if not submission.stickied: # ignoring stickies
                downs_list.append(submission.downs)
                ups_list.append(submission.ups)
                comment_date_list.append(datetime.datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'))        
                res = analyzer.polarity_scores(submission.title) # analyzing headlines
                name_list.append(subreddit.display_name)
                type_list.append('Submission')
                pos_list.append(res['pos'])
                neg_list.append(res['neg'])
                neu_list.append(res['neu'])
                comp_list.append(res['compound'])        
                for comment in submission.comments.list()[:5000]:
                    try:
                        downs_list.append(comment.downs)
                        ups_list.append(comment.ups)
                        comment_date_list.append(datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'))
                        res = analyzer.polarity_scores(comment.body[:500])# analyzing comments
                        name_list.append(subreddit.display_name)
                        type_list.append('Comment')
                        pos_list.append(res['pos'])
                        neg_list.append(res['neg'])
                        neu_list.append(res['neu'])
                        comp_list.append(res['compound'])  
                    except:
                        pass
    except:
        ####### Sending Alert
        fromaddr = "test@test.com"
        toaddr = "to_test@outlook.com"
        msg = MIMEMultipart()
        msg['From'] = "test@test.com"
        msg['To'] = "to_test@outlook.com"
        msg['Subject'] = "sentiment failed"
        body='######################## sentiment failed ######################'
        body = body + "\n" + "\n" + i
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('', 587)
        server.starttls()
        server.login('', "+")
         
        text = msg.as_string()
        
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
                
# creating and filling dataframe from lists
df = pd.DataFrame({'Subreddit_Name':name_list
                  ,'Type':type_list
                  ,'Ups':ups_list
                  ,'Downs':downs_list
                  ,'Comment_Date':comment_date_list
                  ,'Positive':pos_list
                  ,'Negative':neg_list
                  ,'Neutral':neu_list
                  ,'Compound':comp_list
                  ,'Create_Date':date
                  ,'Create_User':'1'
                  ,'Update_Date':date
                  ,'Update_User':'1'})

# creating final dataframe for loading into database    
df =  df [['Subreddit_Name'
          ,'Type'
          ,'Ups'
          ,'Downs'
          ,'Comment_Date'
          ,'Positive'
          ,'Negative'
          ,'Neutral'
          ,'Compound'
          ,'Create_Date'
          ,'Create_User'
          ,'Update_Date'
          ,'Update_User']]

# printing final results   
print (df.head())
print (df.tail())

# dataframe to sql table
engine = create_engine('mysql+mysqlconnector://', echo=False)
df.to_sql(name='Reddit_Sentiment', con=engine, if_exists = 'append', index=False)

print ('Done!')
