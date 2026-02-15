"""
Configuration file for Customer Churn Analytics
Author: T Samuel Paul

INSTRUCTIONS:
1. Copy this file to config.py
2. Update with your actual database credentials
3. Never commit config.py to version control (it's in .gitignore)
"""

# Database Configuration
DB_CONFIG = {
    'server': 'your-server.database.windows.net',
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# Model Parameters
MODEL_CONFIG = {
    'test_size': 0.2,
    'random_state': 42,
    'n_estimators': 200,
    'max_depth': 15,
    'min_samples_split': 20,
    'min_samples_leaf': 10
}

# Risk Thresholds
RISK_THRESHOLDS = {
    'high_risk': 0.70,   # Churn probability >= 70%
    'medium_risk': 0.40,  # Churn probability >= 40%
    'low_risk': 0.00      # Churn probability < 40%
}

# File Paths
PATHS = {
    'raw_data': 'data/raw/',
    'processed_data': 'data/processed/',
    'models': 'models/',
    'reports': 'reports/',
    'logs': 'logs/'
}
