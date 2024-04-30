import streamlit as st
import pandas as pd
import numpy as np
import functions as func

import praw
from kafka import KafkaProducer
from json import dumps, loads
import json

reddit = praw.Reddit(
    client_id="VYwI_9Xqdf4-j6YbAcIXCA",
    client_secret="OvQUVB1QMNs0Xuo9tkOQ9BqVxH-Kmg",
    password="Billion@99",
    user_agent="my_bigdata",
    username="v1nomad",
)

def fetch_data_from_reddit(subreddit_name, keywords):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.top(limit=100):
        if all(keyword.lower() in submission.title.lower() for keyword in keywords):
            post_id = submission.id
            title = submission.title
            url = submission.url
            score = submission.score
            upvotes = submission.ups
            downvotes = submission.downs
            num_comments = submission.num_comments
            text = submission.selftext
            author = submission.author.name if submission.author else None
            author_post_karma = None
            if submission.author:
                try:
                    author_info = reddit.redditor(submission.author.name)
                    author_post_karma = author_info.link_karma + author_info.comment_karma
                except AttributeError:
                # Handle the case where karma retrieval fails
                    author_post_karma = None
            tag = submission.link_flair_text if submission.link_flair_text else None
            comments_data = []
            for comment in submission.comments:
                if isinstance(comment, praw.models.MoreComments):
                    continue  # Skip MoreComments objects
                comment_data = {
                    'comment_id': comment.id,
                    'author': comment.author.name if comment.author else None,
                    'datetime': comment.created_utc,
                    'text': comment.body
                }
                comments_data.append(comment_data)

            yield {
                'post_id': post_id,
                'title': title,
                'url': url,
                'score': score,
                'upvotes': upvotes,
                'downvotes': downvotes,
                'num_comments': num_comments,
                'text': text,
                'author': author,
                'author_post_karma': author_post_karma,
                'tag': tag,
                'comments': comments_data
            }

def kafka_producer(subreddit_name, keywords):
    producer = KafkaProducer(bootstrap_servers=['18.234.36.200:9092'])
    for data in fetch_data_from_reddit(subreddit_name, keywords):
        serialized_data = json.dumps(data).encode('utf-8')
        producer.send('technot', serialized_data)
    producer.flush()


def main():

    """subreddit_name = 'technology'
    keywords_input = input("Enter keywords separated by commas: ")
    keywords = [keyword.strip() for keyword in keywords_input.split(",")]
    kafka_producer(subreddit_name, keywords)"""

    
    # Set page config to wider layout
    st.set_page_config(layout="wide")
    # Import CSS file
    with open("DashStyles.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    st.title('Social Media Listening Dashboard')
    st.sidebar.image('reddit-logo-new.svg', width=200)
    st.sidebar.title('Search Bar')

    # Add one selection field and a button on the sidebar. The selection field should hold keywords that the user can search for.
    # keyword = st.sidebar.selectbox('Select a keyword', ['AI', 'Politics', 'Software', 'Security', 'Business'])
    keyword = st.sidebar.text_input('Enter a keyword')
    keywords = [keyword.strip() for keyword in keywords_input.split(",")]
    #when button pressed
    #kafka_producer(subreddit_name, keywords)
    button = st.sidebar.button('Search')

    row0 = st.columns(1)
    row1 = st.columns(1)
    row2 = st.columns(1)
    posts = None

    if "isExecuted" not in st.session_state:
        st.session_state.isExecuted = 0

    if button:
        
        kafka_producer(subreddit_name, keywords)
        st.session_state.isExecuted = 1
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Word Analysis","Sentiment Analysis", "Sentiment Trends", "Market Funnel", "Comment Analysis"])
        if keyword:
            if st.session_state.isExecuted == 1:
                st.session_state.isExecuted = 0
                posts = func.createDFfromJSON(keyword)
                posts = func.dataCleaning(posts)
                reach, engagement, share_of_voice = func.get_metrics(posts)
                st.sidebar.write('Search completed.')
                st.sidebar.write(f"<div class='summary-header'>Reach (Unique Users)</div>", unsafe_allow_html=True)
                st.sidebar.write(f"<div class='summary-text'>{reach}</div>", unsafe_allow_html=True)
                st.sidebar.markdown("<br>", unsafe_allow_html=True)
                st.sidebar.write(f"<div class='summary-header'>Engagement</div>", unsafe_allow_html=True)
                st.sidebar.write(f"<div class='summary-text'>{engagement}</div>", unsafe_allow_html=True)
                st.sidebar.markdown("<br>", unsafe_allow_html=True)
                st.sidebar.write(f"<div class='summary-header'>Share of Voice</div>", unsafe_allow_html=True)
                st.sidebar.write(f"<div class='summary-text'>{share_of_voice}</div>", unsafe_allow_html=True)
                st.sidebar.markdown("<br>", unsafe_allow_html=True)
                
                with row0[0]:
                    st.write("""Social media listening is the practice of monitoring social media channels of a particular
                            brand/products, competitor presence and related keywords. Brands and products can benefit greatly
                            from understanding what their existing and potential users have to say since they can address feedback
                            and incorporate improvements to achieve better favorability from their target populations.""")
                    st.write(f"<div class='search-title'>Search Key: {keyword}</div>", unsafe_allow_html=True)
                with row1[0]:
                    with tab1:
                        row_0 = st.columns(1)
                        row_1 = st.columns(1)
                        with row_0[0]:
                            func.generate_word_histogram(posts) 
                        with row_1[0]:
                            func.create_word_cloud(posts)
                    with tab2:
                        row = st.columns(2)
                        with row[0]:
                            func.create_sentiment_plot(posts)
                        with row[1]:
                            func.plot_aspect_sentiment(posts)        
                    with tab3:
                        row = st.columns(2)
                        with row[0]:
                            func.plot_sentiment_over_time(posts)
                        with row[1]:
                            func.plot_avg_sentiment_by_hour(posts)   
                    with tab4:
                        row = st.columns(1)
                        with row[0]:
                            func.create_market_funnel(posts)
                    with tab5:
                        row = st.columns(2)
                        with row[0]:
                            func.plot_daily_comments_volume(posts)
                        with row[1]:
                            func.plot_comments_by_aspects(posts)
        else:
            st.write('Please select a keyword to search for.')                

if __name__ == "__main__":
    main()
