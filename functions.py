# Subject: Data 225 (Big Data Technologies)
# Group No: 5
# Topic: Real-Time Social Media Listening (Reddit)
# Usage: Functions for Usage with Reddit Dashboard Streamlit App
# -------------------------------------------------

# Import Relevant Libraries
import json
import os
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import plotly.express as px
from wordcloud import WordCloud
import plotly.graph_objects as go
from collections import Counter
import re
import praw
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps, loads
import json
import threading
import time
import nltk
nltk_data_dir = "./resources/nltk_data_dir/"

# Adding additional logic to allow deployment to Streamlit Cloud for NLTK library
if not os.path.exists(nltk_data_dir):
    os.makedirs(nltk_data_dir, exist_ok=True)
nltk.data.path.clear()
nltk.data.path.append(nltk_data_dir)
nltk.download("stopwords", download_dir=nltk_data_dir)
from nltk.corpus import stopwords


# Initialize Reddit API
reddit = praw.Reddit(
    client_id="VYwI_9Xqdf4-j6YbAcIXCA",
    client_secret="OvQUVB1QMNs0Xuo9tkOQ9BqVxH-Kmg",
    password="Billion@99",
    user_agent="my_bigdata",
    username="v1nomad",
)

# Function to fetch data from Reddit
def fetch_data_from_reddit(subreddit_name, keywords):
    """
    Fetches data from Reddit based on the given subreddit name and keywords.

    Args:
        subreddit_name (str): The name of the subreddit to fetch data from.
        keywords (list): A list of keywords to search for in the subreddit.

    Yields:
        dict: A dictionary containing the fetched data for each submission.

    """
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.search(' '.join(keywords), time_filter='month', sort='new', limit=50):
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

# Start Kafka Producer
def kafka_producer(subreddit_name, keywords):
    """
    Sends data from Reddit to a Kafka topic.

    Args:
        subreddit_name (str): The name of the subreddit to fetch data from.
        keywords (list): A list of keywords to filter the data.

    Returns:
        None
    """
    producer = KafkaProducer(bootstrap_servers=['18.234.36.200:9092'])
    for data in fetch_data_from_reddit(subreddit_name, keywords):
        serialized_data = json.dumps(data).encode('utf-8')
        producer.send('technot', serialized_data)
    producer.flush()
    producer.close()

def start_producer(keywords_input, producer_done_event):
    """
    Starts the producer to fetch data from the 'technology' subreddit based on the given keywords.

    Args:
        keywords_input (str): A comma-separated string of keywords.
        producer_done_event (threading.Event): An event object to signal when the producer is done.

    Returns:
        None
    """
    subreddit_name = 'technology'
    keywords = [keyword.strip() for keyword in keywords_input.split(",")]
    kafka_producer(subreddit_name, keywords)
    producer_done_event.set()

# Start Kafka Consumer
def start_consumer(data_list, producer_done_event):
    """
    Starts a Kafka consumer that listens for messages on the 'technot' topic.

    Args:
        data_list (list): A list to store the received data.
        producer_done_event (threading.Event): An event to signal when the producer is done.

    Returns:
        None
    """
    consumer = KafkaConsumer('technot', bootstrap_servers=['18.234.36.200:9092'])
    try:
        while not producer_done_event.is_set():
            batch = consumer.poll(timeout_ms=1000)
            for tp, messages in batch.items():
                for message in messages:
                    data = loads(message.value.decode('utf-8'))
                    data_list.append(data)
                if len(batch) == 0:
                    print("No new messages received.")    
    except KeyboardInterrupt:
        pass  # Catch KeyboardInterrupt to gracefully stop the consumer
    finally:
        consumer.close()

# Call Producer and Consumer
def start_data_fetch(keywords_input):
    """
    Fetches data using a producer-consumer pattern.

    Args:
        keywords_input (str): The input keywords for data fetching.

    Returns:
        pandas.DataFrame: The fetched data in a DataFrame format.
    """
    producer_done_event = threading.Event() 
    data_list = []
    producer_thread = threading.Thread(target=start_producer, args=(keywords_input,producer_done_event))
    consumer_thread = threading.Thread(target=start_consumer, args=(data_list, producer_done_event))
    producer_thread.start()
    consumer_thread.start()
    producer_thread.join()
    consumer_thread.join()
    if not data_list:
        return pd.DataFrame()
    else:
        df = pd.json_normalize(data_list,'comments', 
                                ['post_id', 'title', 'url', 'score', 'upvotes', 'downvotes', 
                                'num_comments', 'text', 'author', 'author_post_karma', 'tag'], 
                                record_prefix='comment_')
        return df

# Data Cleaning and Preprocessing
def dataCleaning(posts):
    """
    Perform data cleaning and preprocessing on the given DataFrame 'posts'.

    Args:
        posts (DataFrame): The input DataFrame containing the posts data.

    Returns:
        DataFrame: The cleaned and preprocessed DataFrame.

    """

    analyzer = SentimentIntensityAnalyzer()

    # Apply sentiment analysis to the comments
    posts['sentiment'] = posts['comment_text'].apply(lambda x: analyzer.polarity_scores(x)['compound'])

    # Categorize sentiments into positive, neutral, or negative
    posts['sentiment_type'] = posts['sentiment'].apply(lambda x: 'positive' if x > 0.05 else ('neutral' if x > -0.05 else 'negative'))
    
    # Convert datetime for each comment and aggregate by date
    posts['datetime'] = pd.to_datetime(posts['comment_datetime'], unit='s')

    # Assuming you already have a DataFrame 'posts' with a column 'comment_text'
    aspect_terms = {
        'battery': ['battery', 'charge', 'power', 'battery life', 'charging', 'battery capacity',
                    'battery drain', 'recharge', 'lasting power', 'energy', 'power consumption',
                    'charge time', 'rechargeable'],
        'design': ['look', 'design', 'style', 'aesthetic', 'appearance', 'layout', 'build quality',
                'form factor', 'ergonomics', 'styling', 'crafted', 'look and feel', 'sleek'],
        'performance': ['fast', 'performance', 'speed', 'speed up', 'slow down', 'performance issues',
                        'efficient', 'optimization', 'lag', 'responsiveness', 'processing power',
                        'run faster', 'perform well'],
        'camera': ['camera', 'photo quality', 'megapixels', 'low light', 'shutter speed', 'lens',
                'optical zoom', 'image stabilization', 'selfie', 'video recording'],
        'software': ['operating system', 'software update', 'firmware', 'user interface', 'apps',
                    'features', 'bug', 'glitch', 'update', 'version'],
        'sound quality': ['sound', 'volume', 'audio quality', 'speakers', 'bass', 'treble',
                        'acoustic', 'noise cancellation', 'headphones', 'surround sound']
    }

    def categorize_by_aspect(text):
        categories = []
        for aspect, keywords in aspect_terms.items():
            if any(keyword in text for keyword in keywords):
                categories.append(aspect)
        return categories

    # Apply the categorization function
    posts['aspects'] = posts['comment_text'].apply(categorize_by_aspect)
    
    # Convert 'datetime' to datetime format if not already
    posts['datetime'] = pd.to_datetime(posts['datetime'])

    # Creating a new DataFrame with hour and weekday extracted
    posts['hour'] = posts['datetime'].dt.hour
    posts['weekday'] = posts['datetime'].dt.day_name()
    
    return posts

# Sentiment Analysis Plot
def create_sentiment_plot(posts):
    """
    Creates an interactive bar plot to visualize the sentiment analysis of comments.

    Parameters:
    - posts (DataFrame): A DataFrame containing the comments and their sentiment type.

    Returns:
    None
    """
    # Group by sentiment type and count
    sentiment_counts = posts['sentiment_type'].value_counts().reset_index()
    sentiment_counts.columns = ['sentiment_type', 'count']

    # Create an interactive bar plot using Plotly
    fig = px.bar(sentiment_counts, x='sentiment_type', y='count', title='Sentiment Analysis of Comments',
                 labels={'sentiment_type': 'Sentiment Type', 'count': 'Number of Comments'},
                 color='sentiment_type', color_discrete_map={'positive': 'green', 'neutral': 'orange', 'negative': 'red'})
    fig.update_layout(height=500, width=500)
    # Display the plot
    st.plotly_chart(fig, use_container_width=True)
    

# Word Cloud
def create_word_cloud(posts):
    """
    Create a word cloud from the comments in the given DataFrame.

    Parameters:
    - posts (DataFrame): A DataFrame containing the comments.

    Returns:
    None
    """
    # Combine all comments into a single string
    text = ' '.join(posts['comment_text'])

    # Create the word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)

    # Create a Matplotlib figure
    fig, ax = plt.subplots(figsize=(10, 5))

    # Display the word cloud on the figure
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')

    # Display the figure in Streamlit
    st.pyplot(fig)

# Metrics Calculation
def get_metrics(posts,keyword):
    """
    Calculate various metrics based on the given posts data.

    Parameters:
    - posts (DataFrame): A DataFrame containing the posts data.

    Returns:
    - reach (int): The number of unique comment authors.
    - engagement (int): The sum of upvotes and number of comments.
    - share_of_voice (float): The percentage of posts that mention "Apple Vision Pro".

    """
    reach = posts['comment_author'].nunique()
    engagement = posts['upvotes'].sum() + posts['num_comments'].sum()
    total_mentions = posts[posts['comment_text'].str.contains(keyword, case=False)].shape[0]
    total_comments = posts.shape[0]
    share_of_voice = round((total_mentions / total_comments) * 100,2)
    return reach, engagement, share_of_voice

# Plotting Market Funnel
def create_market_funnel(posts):
    """
    Creates a market funnel visualization using a Sankey diagram based on the given posts.

    Args:
        posts (DataFrame): A DataFrame containing the posts data.

    Returns:
        None
    """
    awareness_keywords = [
    'heard', 'saw', 'noticed', 'discovered', 'learned about', 'came across',
    'found', 'encountered', 'uncovered', 'aware of',
    'hearing', 'seeing', 'noticing', 'discovering', 'learning about', 'coming across',
    'finding', 'encountering', 'uncovering',
    'have heard', 'have seen', 'have noticed', 'have discovered', 'have learned about', 'have come across',
    'have found', 'have encountered', 'have uncovered', 'have become aware of'
    ]

    consideration_keywords = [
        'considering', 'thinking about', 'might', 'evaluating', 'pondering',
        'contemplating', 'interested in', 'looking at', 'researching', 'debating', 'unsure about',
        'consider', 'think about', 'evaluate', 'ponder',
        'contemplate', 'be interested in', 'look at', 'research', 'debate', 'be unsure about',
        'have considered', 'have thought about', 'have evaluated', 'have pondered',
        'have contemplated', 'have been interested in', 'have looked at', 'have researched', 'have debated', 'have been unsure about'
    ]

    purchase_keywords = [
    'bought', 'purchased', 'ordered', 'acquired', 'invested in', 'secured',
    'grabbed', 'picked up', 'subscribed', 'completed purchase', 'shopped for'
    ]

    awareness_count = posts[posts['comment_text'].str.contains('|'.join(awareness_keywords), case=False)].shape[0]
    consideration_count = posts[posts['comment_text'].str.contains('|'.join(consideration_keywords), case=False)].shape[0]
    purchase_count = posts[posts['comment_text'].str.contains('|'.join(purchase_keywords), case=False)].shape[0]
    
    # Define a dictionary to map link categories to colors
    link_colors = {
        'awareness_consideration': 'lightskyblue',
        'consideration_purchase': 'lightgreen',
        'purchase_loyalty': 'lightgoldenrodyellow'
    }

    # Example Sankey diagram setup
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=["Awareness", "Consideration", "Purchase", "Loyalty"],
            color=["blue", "green", "orange", "red"],
            customdata=[1, 2, 3, 4],
            hovertemplate='%{value} users reached %{label}<extra></extra>'
        ),
        link=dict(
            source=[0, 1, 2],
            target=[1, 2, 3],
            value=[awareness_count, consideration_count, purchase_count],
            color=[
                link_colors['awareness_consideration'],
                link_colors['consideration_purchase'],
                link_colors['purchase_loyalty']
            ]
        )
    )])

    # Update layout to customize font size of labels
    fig.update_layout(
        title_text="Marketing Funnel Transitions",
        font=dict(size=18),
        hoverlabel=dict(font_size=20)
    )
    fig.update_layout(height=500, width=1000)
    # Display the Sankey diagram in Streamlit
    st.plotly_chart(fig)

# Aspect Sentiment Analysis Plot
def plot_aspect_sentiment(posts):
    """
    Plots the sentiment distribution by aspects.

    Args:
        posts (DataFrame): The input DataFrame containing the posts.

    Returns:
        None
    """
    # Explode the DataFrame to create individual rows for each aspect
    exploded_df = posts.explode('aspects')

    # Aspect-based Sentiment Analysis
    aspect_sentiment = exploded_df.groupby(['aspects', 'sentiment_type']).size().unstack().fillna(0)

    # Convert DataFrame to Plotly format
    data = []
    colors = {'positive': 'green', 'negative': 'red', 'neutral': 'orange'}
    for sentiment_type in aspect_sentiment.columns:
        data.append(go.Bar(name=sentiment_type, x=aspect_sentiment.index, y=aspect_sentiment[sentiment_type],
                           marker=dict(color=colors[sentiment_type])))

    # Create the Plotly figure
    fig = go.Figure(data=data)
    fig.update_layout(barmode='stack', title='Sentiment Distribution by Aspects',
                      xaxis=dict(title='Aspects'), yaxis=dict(title='Number of Comments'))
    fig.update_layout(height=500, width=500)
    # Display the Plotly figure in Streamlit
    st.plotly_chart(fig)
    
# Sentiment Trends Over Time
def plot_sentiment_over_time(posts):
    """
    Plots the sentiment type trends over time based on the given posts.

    Parameters:
    - posts (DataFrame): A pandas DataFrame containing the posts data.

    Returns:
    - None

    Example Usage:
    >>> plot_sentiment_over_time(posts)
    """

    # Resample and count sentiment types over time
    sentiment_over_time = posts.groupby('sentiment_type').resample('W', on='datetime').size().unstack(0).fillna(0)

    # Convert the index to datetime if not already in datetime format
    sentiment_over_time.index = pd.to_datetime(sentiment_over_time.index)

    # Create the Plotly traces
    data = []
    
    colors = {'positive': 'green', 'negative': 'red', 'neutral': 'orange'}
    
    for sentiment in sentiment_over_time.columns:
        data.append(go.Scatter(x=sentiment_over_time.index, y=sentiment_over_time[sentiment], mode='lines',
                               name=sentiment, line=dict(color=colors[sentiment])))
    
    # Create the Plotly layout
    layout = go.Layout(title='Sentiment Type Trends Over Time', xaxis=dict(title='Time'),
                       yaxis=dict(title='Number of Comments'), legend=dict(title='Sentiment Type'),
                       height=500, width=450)

    # Create the Plotly figure
    fig = go.Figure(data=data, layout=layout)

    # Display the Plotly figure in Streamlit
    st.plotly_chart(fig)
    
# Plotting Daily Comments Volume
def plot_daily_comments_volume(posts):
    """
    Plots the daily volume of comments from a DataFrame of posts.

    Parameters:
    - posts: DataFrame
        The DataFrame containing the posts data.

    Returns:
    None
    """
    daily_comments = posts.groupby(posts['datetime'].dt.date).size()
    # Plotting daily comments
    daily_comments.plot(kind='line', figsize=(4, 4), title='Daily Comment Volume')
    
    fig = px.line(daily_comments, x=daily_comments.index, y=daily_comments.values, title='Daily Comments Volume')
    fig.update_layout(height=500, width=450)
    fig.update_xaxes(title_text='Date')
    fig.update_yaxes(title_text='Number of Comments')
    st.plotly_chart(fig)

# Plotting Average Sentiment by Hour
def plot_avg_sentiment_by_hour(posts):
    """
    Plots the average sentiment by hour across days of the week.

    Args:
        posts (DataFrame): The DataFrame containing the posts data.

    Returns:
        None
    """
    # Creating a pivot table of average sentiment over hours and days
    pivot_table = posts.pivot_table(values='sentiment', index='hour', columns='weekday', aggfunc='mean')

    # Convert day names to numbers
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_table.columns = pd.CategoricalIndex(pivot_table.columns, categories=days_of_week, ordered=True)

    # Create traces for each day
    traces = []
    for day in days_of_week:
        if day in pivot_table.columns:
            trace = go.Scatter(x=pivot_table.index, y=pivot_table[day], mode='lines', name=day)
            traces.append(trace)

    # Create layout
    layout = go.Layout(title='Average Sentiment by Hour Across Days of the Week',
                       xaxis=dict(title='Hour of Day', tickvals=list(range(24)), ticktext=list(range(1, 25))),
                       yaxis=dict(title='Average Sentiment'), legend=dict(title='Day of the Week'),
                       hovermode='closest', height=500, width=500)

    # Create figure
    fig = go.Figure(data=traces, layout=layout)

    # Display the Plotly figure in Streamlit
    st.plotly_chart(fig)

# Plotting Comments by Aspects
def plot_comments_by_aspects(posts):
    """
    Plots the distribution of comments by aspects using Plotly.

    Args:
        posts (DataFrame): A DataFrame containing posts and their aspects.

    Returns:
        None
    """
    # Filter out posts with no matched aspects
    filtered_posts = posts[posts['aspects'].map(len) > 0]

    # Count the occurrences of each aspect
    aspect_counts = filtered_posts['aspects'].explode().value_counts()

    # Convert aspect_counts to a DataFrame for easier plotting with Plotly
    aspect_counts_df = pd.DataFrame({'Aspect': aspect_counts.index, 'Number of Comments': aspect_counts.values})

    # Define a custom color palette with rainbow-like colors
    custom_colors = px.colors.qualitative.Light24

    # Create a Plotly bar chart with custom color palette
    fig = px.bar(aspect_counts_df, x='Aspect', y='Number of Comments', 
                 title='Distribution of Comments by Aspects',
                 labels={'Aspect': 'Aspects', 'Number of Comments': 'Number of Comments'},
                 color='Aspect', color_discrete_sequence=custom_colors)
    fig.update_layout(height=500, width=500)
    # Customize layout if needed
    fig.update_layout(xaxis_tickangle=-45)

    # Display the Plotly chart in Streamlit
    st.plotly_chart(fig)

# Generate Word Histogram
def generate_word_histogram(posts):
    """
    Generate a word histogram from the comments in the given DataFrame.

    Parameters:
    - posts (DataFrame): A DataFrame containing comment data.

    Returns:
    None
    """

    # Concatenate all comments into a single string
    all_comments = ' '.join(posts['comment_text'].dropna())

    # Tokenize the comment text to extract individual words
    words = re.findall(r'\b\w+\b', all_comments.lower())

    # Filter out stop words
    stop_words = set(stopwords.words('english'))
    words = [word for word in words if word not in stop_words]

    # Count the occurrences of each word
    word_counts = Counter(words)

    # Select the top 25 most common words
    top_words = word_counts.most_common(25)

    # Convert the top words data to a DataFrame
    top_words_df = pd.DataFrame(top_words, columns=['Word', 'Count'])

    # Create a Plotly histogram
    fig = px.bar(top_words_df, x='Word', y='Count', 
                 title='Top 25 Most Common Words in Comments (excluding stop words)',
                 labels={'Word': 'Word', 'Count': 'Count'},
                 color='Count', color_continuous_scale='thermal')

    # Customize layout if needed
    fig.update_layout(xaxis_tickangle=-45)
    fig.update_layout(height=400, width=1000)

    # Display the Plotly chart in Streamlit
    st.plotly_chart(fig)
