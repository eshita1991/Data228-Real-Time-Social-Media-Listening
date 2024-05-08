# Subject: Data 225 (Big Data Technologies)
# Group No: 5
# Topic: Real-Time Social Media Listening (Reddit)
# Usage: Main file to run the Streamlit dashboard
# -------------------------------------------------

import streamlit as st
import pandas as pd
import numpy as np
import functions as func

def main():
    """
    The main function that sets up the Social Media Listening Dashboard.

    This function sets the page configuration to a wider layout, imports a CSS file,
    and creates the main components of the dashboard, including the title, sidebar,
    keyword selection field, and search button. It also handles the search functionality
    and displays the results in different tabs.

    Args:
        None

    Returns:
        None
    """
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
    button = st.sidebar.button('Search')

    row0 = st.columns(1)
    row1 = st.columns(1)
    row2 = st.columns(1)
    posts = None

    if "isExecuted" not in st.session_state:
        st.session_state.isExecuted = 0
        st.cache_data.clear()

    if button:
        st.session_state.isExecuted = 1
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Word Analysis","Sentiment Analysis", "Sentiment Trends", "Market Funnel", "Comment Analysis"])
        if keyword:
            if st.session_state.isExecuted == 1:
                st.session_state.isExecuted = 0
                posts = func.start_data_fetch(keyword)
                if len(posts) == 0:
                    st.write('No data found for the keyword. Please try another keyword.')
                    return
                posts = func.dataCleaning(posts)
                reach, engagement, share_of_voice = func.get_metrics(posts,keyword)
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
