"""
Customer Churn Analytics - Data Extraction Module
Author: T Samuel Paul
Description: Extracts customer data from SQL Server for churn prediction
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple
import sys
from config import DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_extraction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles SQL Server data extraction for churn analytics."""
    
    def __init__(self, config: Dict):
        """
        Initialize database connection.
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        self.conn = None
        self.cursor = None
        
    def connect(self) -> None:
        """Establish connection to SQL Server."""
        try:
            conn_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['username']};"
                f"PWD={self.config['password']}"
            )
            self.conn = pyodbc.connect(conn_string)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to SQL Server")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def extract_customer_base(self) -> pd.DataFrame:
        """
        Extract core customer information.
        
        Returns:
            DataFrame with customer demographics and account info
        """
        query = """
        SELECT 
            c.customer_id,
            c.signup_date,
            c.account_status,
            c.country,
            c.company_size,
            c.industry,
            DATEDIFF(day, c.signup_date, GETDATE()) as tenure_days,
            CASE 
                WHEN c.account_status = 'Churned' THEN 1 
                ELSE 0 
            END as churned
        FROM customers c
        WHERE c.signup_date >= DATEADD(year, -2, GETDATE())
        """
        
        logger.info("Extracting customer base data...")
        df = pd.read_sql(query, self.conn)
        logger.info(f"Extracted {len(df):,} customer records")
        return df
    
    def extract_usage_metrics(self) -> pd.DataFrame:
        """
        Extract product usage and engagement metrics.
        
        Returns:
            DataFrame with aggregated usage statistics
        """
        query = """
        WITH usage_stats AS (
            SELECT 
                customer_id,
                COUNT(DISTINCT session_id) as total_sessions,
                COUNT(DISTINCT DATE(login_date)) as active_days,
                AVG(session_duration_minutes) as avg_session_duration,
                MAX(login_date) as last_login_date,
                COUNT(DISTINCT feature_id) as unique_features_used,
                SUM(CASE WHEN event_type = 'feature_adoption' THEN 1 ELSE 0 END) as feature_adoptions
            FROM user_activity
            WHERE login_date >= DATEADD(month, -6, GETDATE())
            GROUP BY customer_id
        )
        SELECT 
            customer_id,
            COALESCE(total_sessions, 0) as total_sessions,
            COALESCE(active_days, 0) as active_days,
            COALESCE(avg_session_duration, 0) as avg_session_duration,
            DATEDIFF(day, last_login_date, GETDATE()) as days_since_last_login,
            COALESCE(unique_features_used, 0) as unique_features_used,
            COALESCE(feature_adoptions, 0) as feature_adoptions,
            CASE 
                WHEN active_days > 0 THEN CAST(total_sessions AS FLOAT) / active_days
                ELSE 0 
            END as sessions_per_active_day
        FROM usage_stats
        """
        
        logger.info("Extracting usage metrics...")
        df = pd.read_sql(query, self.conn)
        logger.info(f"Extracted usage data for {len(df):,} customers")
        return df
    
    def extract_financial_metrics(self) -> pd.DataFrame:
        """
        Extract revenue and payment information.
        
        Returns:
            DataFrame with financial metrics
        """
        query = """
        SELECT 
            customer_id,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_monthly_revenue,
            COUNT(*) as total_invoices,
            SUM(CASE WHEN payment_status = 'Late' THEN 1 ELSE 0 END) as late_payments,
            SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) as failed_payments,
            MAX(invoice_date) as last_invoice_date,
            CASE 
                WHEN COUNT(*) > 0 
                THEN CAST(SUM(CASE WHEN payment_status = 'Paid' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)
                ELSE 0 
            END as payment_success_rate
        FROM invoices
        WHERE invoice_date >= DATEADD(month, -12, GETDATE())
        GROUP BY customer_id
        """
        
        logger.info("Extracting financial metrics...")
        df = pd.read_sql(query, self.conn)
        logger.info(f"Extracted financial data for {len(df):,} customers")
        return df
    
    def extract_support_metrics(self) -> pd.DataFrame:
        """
        Extract customer support interaction data.
        
        Returns:
            DataFrame with support ticket statistics
        """
        query = """
        SELECT 
            customer_id,
            COUNT(*) as total_tickets,
            AVG(DATEDIFF(hour, created_date, resolved_date)) as avg_resolution_hours,
            SUM(CASE WHEN priority = 'High' THEN 1 ELSE 0 END) as high_priority_tickets,
            SUM(CASE WHEN status = 'Escalated' THEN 1 ELSE 0 END) as escalated_tickets,
            MAX(created_date) as last_ticket_date
        FROM support_tickets
        WHERE created_date >= DATEADD(month, -6, GETDATE())
        GROUP BY customer_id
        """
        
        logger.info("Extracting support metrics...")
        df = pd.read_sql(query, self.conn)
        logger.info(f"Extracted support data for {len(df):,} customers")
        return df
    
    def extract_engagement_metrics(self) -> pd.DataFrame:
        """
        Extract email and communication engagement data.
        
        Returns:
            DataFrame with engagement statistics
        """
        query = """
        SELECT 
            customer_id,
            COUNT(*) as emails_sent,
            SUM(CASE WHEN opened = 1 THEN 1 ELSE 0 END) as emails_opened,
            SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) as emails_clicked,
            CASE 
                WHEN COUNT(*) > 0 
                THEN CAST(SUM(CASE WHEN opened = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)
                ELSE 0 
            END as open_rate,
            AVG(nps_score) as avg_nps_score
        FROM marketing_engagement
        WHERE sent_date >= DATEADD(month, -6, GETDATE())
        GROUP BY customer_id
        """
        
        logger.info("Extracting engagement metrics...")
        df = pd.read_sql(query, self.conn)
        logger.info(f"Extracted engagement data for {len(df):,} customers")
        return df
    
    def merge_datasets(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Merge all extracted datasets into single DataFrame.
        
        Args:
            dataframes: List of DataFrames to merge
            
        Returns:
            Merged DataFrame with all features
        """
        logger.info("Merging datasets...")
        base_df = dataframes[0]
        
        for df in dataframes[1:]:
            base_df = base_df.merge(df, on='customer_id', how='left')
        
        # Fill missing values with 0 for metric columns
        numeric_cols = base_df.select_dtypes(include=[np.number]).columns
        base_df[numeric_cols] = base_df[numeric_cols].fillna(0)
        
        logger.info(f"Final dataset shape: {base_df.shape}")
        logger.info(f"Total features: {len(base_df.columns)}")
        
        return base_df
    
    def save_data(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
        """
        df.to_csv(filepath, index=False)
        logger.info(f"Data saved to {filepath}")
        logger.info(f"Rows: {len(df):,} | Columns: {len(df.columns)}")


def main():
    """Main execution function."""
    try:
        # Initialize extractor
        extractor = DataExtractor(DB_CONFIG)
        extractor.connect()
        
        # Extract all datasets
        customer_df = extractor.extract_customer_base()
        usage_df = extractor.extract_usage_metrics()
        financial_df = extractor.extract_financial_metrics()
        support_df = extractor.extract_support_metrics()
        engagement_df = extractor.extract_engagement_metrics()
        
        # Merge datasets
        final_df = extractor.merge_datasets([
            customer_df, 
            usage_df, 
            financial_df, 
            support_df, 
            engagement_df
        ])
        
        # Save to file
        output_path = f'data/raw/customer_data_{datetime.now().strftime("%Y%m%d")}.csv'
        extractor.save_data(final_df, output_path)
        
        # Print summary statistics
        print("\n" + "="*50)
        print("DATA EXTRACTION SUMMARY")
        print("="*50)
        print(f"Total Customers: {len(final_df):,}")
        print(f"Churned Customers: {final_df['churned'].sum():,}")
        print(f"Churn Rate: {final_df['churned'].mean()*100:.2f}%")
        print(f"Total Features: {len(final_df.columns)}")
        print(f"Date Range: {datetime.now().strftime('%Y-%m-%d')}")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        extractor.disconnect()


if __name__ == "__main__":
    main()
