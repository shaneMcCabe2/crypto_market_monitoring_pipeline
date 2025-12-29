"""
Generate insights and visualizations from crypto pipeline data
"""

from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# Initialize BigQuery client
project_id = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project=project_id)


def get_top_coins_by_market_cap(limit=10):
    """Get top coins by current market cap"""
    query = f"""
    WITH latest_prices AS (
        SELECT 
            p.coin_key,
            p.current_price,
            p.market_cap,
            p.price_change_percentage_24h,
            ROW_NUMBER() OVER (PARTITION BY p.coin_key ORDER BY t.timestamp DESC) as rn
        FROM `{project_id}.crypto_pipeline.fact_price_snapshots` p
        JOIN `{project_id}.crypto_pipeline.dim_timestamp` t
            ON p.timestamp_key = t.timestamp_key
    )
    SELECT 
        c.name,
        c.symbol,
        p.current_price,
        p.market_cap,
        p.price_change_percentage_24h
    FROM latest_prices p
    JOIN `{project_id}.crypto_pipeline.dim_coin` c 
        ON p.coin_key = c.coin_key
    WHERE p.rn = 1
    ORDER BY p.market_cap DESC
    LIMIT {limit}
    """
    
    df = client.query(query).to_dataframe()
    return df


def get_price_trends():
    """Get price trends over time for top 5 coins"""
    query = f"""
    WITH latest_snapshot AS (
        SELECT MAX(timestamp_key) as max_key
        FROM `{project_id}.crypto_pipeline.fact_price_snapshots`
    ),
    top_coins AS (
        SELECT p.coin_key
        FROM `{project_id}.crypto_pipeline.fact_price_snapshots` p
        CROSS JOIN latest_snapshot l
        WHERE p.timestamp_key = l.max_key
        ORDER BY p.market_cap DESC
        LIMIT 5
    )
    SELECT 
        c.name,
        t.timestamp,
        p.current_price
    FROM `{project_id}.crypto_pipeline.fact_price_snapshots` p
    JOIN `{project_id}.crypto_pipeline.dim_coin` c ON p.coin_key = c.coin_key
    JOIN `{project_id}.crypto_pipeline.dim_timestamp` t ON p.timestamp_key = t.timestamp_key
    WHERE p.coin_key IN (SELECT coin_key FROM top_coins)
    ORDER BY t.timestamp, c.name
    """
    
    df = client.query(query).to_dataframe()
    return df


def get_sentiment_correlation():
    """Get sentiment score with average price changes"""
    query = f"""
    SELECT 
        s.sentiment_value,
        s.sentiment_classification,
        t.date,
        AVG(p.price_change_percentage_24h) as avg_price_change
    FROM `{project_id}.crypto_pipeline.fact_sentiment` s
    JOIN `{project_id}.crypto_pipeline.dim_timestamp` t ON s.timestamp_key = t.timestamp_key
    LEFT JOIN `{project_id}.crypto_pipeline.fact_price_snapshots` p 
        ON s.timestamp_key = p.timestamp_key
    GROUP BY s.sentiment_value, s.sentiment_classification, t.date
    ORDER BY t.date
    """
    
    df = client.query(query).to_dataframe()
    return df


def generate_insights():
    """Generate and save insights"""
    print("=" * 60)
    print("Crypto Market Insights")
    print("=" * 60)
    
    # 1. Top coins by market cap
    print("\n1. Top 10 Cryptocurrencies by Market Cap")
    print("-" * 60)
    top_coins = get_top_coins_by_market_cap(10)
    print(top_coins.to_string(index=False))
    
    # Save visualization
    plt.figure(figsize=(12, 6))
    plt.barh(top_coins['name'], top_coins['market_cap'] / 1e9)
    plt.xlabel('Market Cap (Billions USD)')
    plt.title('Top 10 Cryptocurrencies by Market Cap')
    plt.tight_layout()
    plt.savefig('analytics/top_coins_market_cap.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: analytics/top_coins_market_cap.png")
    plt.close()
    
    # 2. Price trends
    print("\n2. Price Trends (Top 5 Coins)")
    print("-" * 60)
    trends = get_price_trends()
    
    if not trends.empty:
        plt.figure(figsize=(14, 7))
        for coin in trends['name'].unique():
            coin_data = trends[trends['name'] == coin]
            plt.plot(coin_data['timestamp'], coin_data['current_price'], 
                    marker='o', label=coin, linewidth=2)
        
        plt.xlabel('Time')
        plt.ylabel('Price (USD)')
        plt.title('Price Trends Over Time')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('analytics/price_trends.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: analytics/price_trends.png")
        plt.close()
    
    # 3. Sentiment analysis
    print("\n3. Market Sentiment vs Price Changes")
    print("-" * 60)
    sentiment = get_sentiment_correlation()
    
    if not sentiment.empty:
        print(f"Average sentiment: {sentiment['sentiment_value'].mean():.0f}")
        print(f"Sentiment classification: {sentiment['sentiment_classification'].mode()[0]}")
        print(f"Average 24h price change: {sentiment['avg_price_change'].mean():.2f}%")
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Sentiment Score', color='tab:blue')
        ax1.plot(sentiment['date'], sentiment['sentiment_value'], 
                color='tab:blue', marker='o', label='Sentiment Score')
        ax1.tick_params(axis='y', labelcolor='tab:blue')
        
        ax2 = ax1.twinx()
        ax2.set_ylabel('Avg Price Change (%)', color='tab:red')
        ax2.plot(sentiment['date'], sentiment['avg_price_change'], 
                color='tab:red', marker='s', label='Avg Price Change')
        ax2.tick_params(axis='y', labelcolor='tab:red')
        
        plt.title('Market Sentiment vs Average Price Changes')
        plt.xticks(rotation=45)
        fig.tight_layout()
        plt.savefig('analytics/sentiment_vs_price.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: analytics/sentiment_vs_price.png")
        plt.close()
    
    print("\n" + "=" * 60)
    print("Analysis complete! Check the analytics/ folder for visualizations.")
    print("=" * 60)


if __name__ == "__main__":
    generate_insights()