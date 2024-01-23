# Set environment
import sys, os
sys.path.append(os.path.expanduser('~/code/IE212.O11.Group11'))

# Import libs
import json
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

# Import custom modules
from _constants import *

def count_words(text):
    words = text.split()
    return len(words)

def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_DETECTED_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

    subreddit_counts = {}
    df = pd.DataFrame(columns=['subreddit', 'total_posts', 'unique_posts', 'unique_stress_count', 'avg_words'])

    for message in consumer:
        subreddit = message.value.get('subreddit')
        post_id = message.value.get('post_id')
        text = message.value.get('text', '')
        label_pred = message.value.get('label_pred')

        num_words = count_words(text)

        if subreddit not in subreddit_counts:
            subreddit_counts[subreddit] = {
                'total_posts': 0,
                'unique_post_ids': set(),
                'unique_total_posts_words': 0,
                'unique_stress_count': 0,
            }

        subreddit_counts[subreddit]['total_posts'] += 1

        if post_id not in subreddit_counts[subreddit]['unique_post_ids']:
            subreddit_counts[subreddit]['unique_post_ids'].add(post_id)
            subreddit_counts[subreddit]['unique_total_posts_words'] += num_words

            if label_pred == 1.0:
                subreddit_counts[subreddit]['unique_stress_count'] += 1

        df.loc[subreddit] = {
            'subreddit': subreddit,
            'total_posts': subreddit_counts[subreddit]['total_posts'],
            'unique_posts': len(subreddit_counts[subreddit]['unique_post_ids']),
            'unique_stress_count': subreddit_counts[subreddit]['unique_stress_count'],
            'avg_words': subreddit_counts[subreddit]['unique_total_posts_words'] / len(subreddit_counts[subreddit]['unique_post_ids']),
        }

        time_div.success(f'Last updated: {message.timestamp}')
        df_tbl.dataframe(df, hide_index=True, use_container_width=True)
        total_posts_chart.bar_chart(df, y=['total_posts'], use_container_width=True)
        unique_posts_chart.bar_chart(df, y=['unique_posts'], use_container_width=True)
        avg_words_chart.bar_chart(df, y=['avg_words'], use_container_width=True)

if __name__ == '__main__':
    # Default settings
    st.set_page_config(
        page_title='Detected Result Visualization',
        layout='wide',
        initial_sidebar_state='expanded'
    )

    # Run the Streamlit app
    st.title('VISUALIZE: STRESSDETECTED RESULT')

    # Add Logo
    st.sidebar.image('./imgs/logo.png', width=250)

    # Sidebar with user instructions
    st.sidebar.markdown(
        '''
        This application is used for visualize the crawled data, and detected result of project:
        \'Real-time Stress Detection on Reddit Posts\'
        '''
    )

    time_div = st.empty()

    df_tbl = st.empty()
    st.markdown('<p style="width:100%;text-align:center;margin:0 0 160px 0">General statistics table</p>', unsafe_allow_html=True)
    
    total_posts_chart = st.empty()
    st.markdown('<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the total posts per subreddit bar chart</p>', unsafe_allow_html=True)
    
    unique_posts_chart = st.empty()
    st.markdown('<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the unique posts per subreddit bar chart</p>', unsafe_allow_html=True)
    
    avg_words_chart = st.empty()
    st.markdown('<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the avarage words of post per subreddit bar chart</p>', unsafe_allow_html=True)

    consume_kafka_messages()