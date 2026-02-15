"""
Customer Churn Analytics - Model Training Module
Author: T Samuel Paul
Description: Trains machine learning model to predict customer churn
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    roc_auc_score,
    roc_curve,
    precision_recall_curve
)
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/model_training.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ChurnModelTrainer:
    """Handles model training and evaluation for churn prediction."""
    
    def __init__(self, data_path: str):
        """
        Initialize the model trainer.
        
        Args:
            data_path: Path to training data CSV
        """
        self.data_path = data_path
        self.model = None
        self.scaler = StandardScaler()
        self.feature_importance = None
        self.metrics = {}
        
    def load_data(self) -> pd.DataFrame:
        """Load training data from CSV."""
        logger.info(f"Loading data from {self.data_path}")
        df = pd.read_csv(self.data_path)
        logger.info(f"Loaded {len(df):,} records with {len(df.columns)} columns")
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """
        Prepare features and target for modeling.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (X_train, X_test, y_train, y_test, feature_names)
        """
        logger.info("Preparing features...")
        
        # Define feature columns (exclude ID and target)
        feature_cols = [col for col in df.columns 
                       if col not in ['customer_id', 'churned', 'signup_date', 
                                     'account_status', 'last_login_date', 
                                     'last_invoice_date', 'last_ticket_date']]
        
        # Separate features and target
        X = df[feature_cols]
        y = df['churned']
        
        # Handle any remaining missing values
        X = X.fillna(0)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        logger.info(f"Training set: {len(X_train):,} samples")
        logger.info(f"Test set: {len(X_test):,} samples")
        logger.info(f"Features: {len(feature_cols)}")
        logger.info(f"Churn rate (train): {y_train.mean()*100:.2f}%")
        logger.info(f"Churn rate (test): {y_test.mean()*100:.2f}%")
        
        return X_train_scaled, X_test_scaled, y_train, y_test, feature_cols
    
    def train_model(self, X_train: np.ndarray, y_train: np.ndarray) -> None:
        """
        Train Random Forest classifier.
        
        Args:
            X_train: Training features
            y_train: Training target
        """
        logger.info("Training Random Forest model...")
        
        # Define model with optimized parameters
        self.model = RandomForestClassifier(
            n_estimators=200,
            max_depth=15,
            min_samples_split=20,
            min_samples_leaf=10,
            max_features='sqrt',
            random_state=42,
            n_jobs=-1,
            class_weight='balanced'
        )
        
        # Train model
        self.model.fit(X_train, y_train)
        logger.info("Model training complete")
        
        # Cross-validation
        cv_scores = cross_val_score(
            self.model, X_train, y_train, 
            cv=5, scoring='roc_auc', n_jobs=-1
        )
        logger.info(f"Cross-validation ROC-AUC: {cv_scores.mean():.4f} (+/- {cv_scores.std()*2:.4f})")
    
    def evaluate_model(self, X_test: np.ndarray, y_test: np.ndarray, 
                      feature_names: list) -> dict:
        """
        Evaluate model performance.
        
        Args:
            X_test: Test features
            y_test: Test target
            feature_names: List of feature names
            
        Returns:
            Dictionary of evaluation metrics
        """
        logger.info("Evaluating model performance...")
        
        # Predictions
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Calculate metrics
        metrics = {
            'accuracy': self.model.score(X_test, y_test),
            'roc_auc': roc_auc_score(y_test, y_pred_proba),
            'confusion_matrix': confusion_matrix(y_test, y_pred),
            'classification_report': classification_report(y_test, y_pred, output_dict=True)
        }
        
        # Feature importance
        self.feature_importance = pd.DataFrame({
            'feature': feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Store metrics
        self.metrics = metrics
        
        # Print results
        print("\n" + "="*60)
        print("MODEL PERFORMANCE METRICS")
        print("="*60)
        print(f"Accuracy:  {metrics['accuracy']:.4f}")
        print(f"ROC-AUC:   {metrics['roc_auc']:.4f}")
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))
        print("\nConfusion Matrix:")
        print(metrics['confusion_matrix'])
        print("\nTop 10 Important Features:")
        print(self.feature_importance.head(10).to_string(index=False))
        print("="*60)
        
        return metrics
    
    def plot_feature_importance(self, top_n: int = 15) -> None:
        """
        Plot feature importance chart.
        
        Args:
            top_n: Number of top features to display
        """
        plt.figure(figsize=(10, 8))
        top_features = self.feature_importance.head(top_n)
        
        sns.barplot(data=top_features, y='feature', x='importance', palette='viridis')
        plt.title(f'Top {top_n} Most Important Features for Churn Prediction', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Importance Score', fontsize=12)
        plt.ylabel('Feature', fontsize=12)
        plt.tight_layout()
        
        output_path = 'reports/feature_importance.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Feature importance plot saved to {output_path}")
        plt.close()
    
    def plot_roc_curve(self, X_test: np.ndarray, y_test: np.ndarray) -> None:
        """
        Plot ROC curve.
        
        Args:
            X_test: Test features
            y_test: Test target
        """
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)
        
        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, linewidth=2, label=f'ROC Curve (AUC = {self.metrics["roc_auc"]:.3f})')
        plt.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random Classifier')
        plt.xlabel('False Positive Rate', fontsize=12)
        plt.ylabel('True Positive Rate', fontsize=12)
        plt.title('ROC Curve - Churn Prediction Model', fontsize=14, fontweight='bold')
        plt.legend(loc='lower right', fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        output_path = 'reports/roc_curve.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"ROC curve saved to {output_path}")
        plt.close()
    
    def plot_confusion_matrix(self) -> None:
        """Plot confusion matrix heatmap."""
        cm = self.metrics['confusion_matrix']
        
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', cbar=True,
                   xticklabels=['No Churn', 'Churn'],
                   yticklabels=['No Churn', 'Churn'])
        plt.title('Confusion Matrix - Churn Prediction', fontsize=14, fontweight='bold')
        plt.ylabel('Actual', fontsize=12)
        plt.xlabel('Predicted', fontsize=12)
        plt.tight_layout()
        
        output_path = 'reports/confusion_matrix.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Confusion matrix saved to {output_path}")
        plt.close()
    
    def save_model(self, model_path: str = 'models/churn_model.pkl') -> None:
        """
        Save trained model to file.
        
        Args:
            model_path: Output file path
        """
        model_artifacts = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_importance': self.feature_importance,
            'metrics': self.metrics,
            'training_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_artifacts, f)
        
        logger.info(f"Model saved to {model_path}")
        
        # Save feature importance to CSV
        self.feature_importance.to_csv('reports/feature_importance.csv', index=False)
        logger.info("Feature importance saved to reports/feature_importance.csv")


def main():
    """Main execution function."""
    try:
        # Initialize trainer
        trainer = ChurnModelTrainer('data/processed/customer_data_cleaned.csv')
        
        # Load and prepare data
        df = trainer.load_data()
        X_train, X_test, y_train, y_test, feature_names = trainer.prepare_features(df)
        
        # Train model
        trainer.train_model(X_train, y_train)
        
        # Evaluate model
        metrics = trainer.evaluate_model(X_test, y_test, feature_names)
        
        # Generate visualizations
        trainer.plot_feature_importance()
        trainer.plot_roc_curve(X_test, y_test)
        trainer.plot_confusion_matrix()
        
        # Save model
        trainer.save_model()
        
        logger.info("Model training pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise


if __name__ == "__main__":
    main()
