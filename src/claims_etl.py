"""
Healthcare Claims Processing ETL Pipeline
==========================================

This module handles the Extract, Transform, and Load (ETL) process for healthcare claims data.
It extracts data from hospital databases, transforms it according to healthcare standards,
and loads it into Azure SQL for analytics and reporting.

Author: Healthcare Data Team
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import yaml
from pathlib import Path
from loguru import logger
import pyodbc
from sqlalchemy import create_engine, text
import hashlib
import json


class ClaimsETLPipeline:
    """
    Main ETL pipeline class for processing healthcare claims data.
    
    This class handles:
    - Extraction from source systems
    - Data validation and cleaning
    - Transformation to standard format
    - Loading into Azure SQL database
    """
    
    def __init__(self, config_path: str = "config/etl_config.yaml"):
        """
        Initialize the ETL pipeline with configuration.
        
        Args:
            config_path: Path to the ETL configuration file
        """
        self.config = self._load_config(config_path)
        self.source_connection = None
        self.target_connection = None
        self.processed_claims = 0
        self.failed_claims = 0
        
        # Setup logging
        logger.add(
            "logs/claims_etl_{time}.log",
            rotation="1 day",
            retention="30 days",
            level="INFO"
        )
        logger.info("Claims ETL Pipeline initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        """
        Load ETL configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dictionary containing configuration parameters
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def extract_claims_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Extract claims data from hospital database.
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            DataFrame containing raw claims data
        """
        logger.info(f"Extracting claims data from {start_date} to {end_date}")
        
        try:
            # Simulate database connection and query
            # In real implementation, this would connect to actual hospital database
            query = f"""
            SELECT 
                claim_id,
                patient_id,
                provider_id,
                service_date,
                procedure_code,
                diagnosis_code,
                amount_billed,
                amount_paid,
                insurance_type,
                claim_status,
                created_date,
                updated_date
            FROM hospital_claims 
            WHERE service_date BETWEEN '{start_date}' AND '{end_date}'
            AND claim_status IN ('PROCESSED', 'PENDING')
            """
            
            # For demo purposes, generate sample data
            sample_data = self._generate_sample_claims_data(start_date, end_date)
            logger.info(f"Extracted {len(sample_data)} claims records")
            
            return sample_data
            
        except Exception as e:
            logger.error(f"Error extracting claims data: {e}")
            raise
    
    def transform_claims_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw claims data to standard format.
        
        This method performs:
        - Data validation and cleaning
        - Standardization of codes and formats
        - Calculation of derived fields
        - Data quality checks
        
        Args:
            raw_data: DataFrame containing raw claims data
            
        Returns:
            DataFrame containing transformed claims data
        """
        logger.info("Starting claims data transformation")
        
        try:
            # Create a copy to avoid modifying original data
            transformed_data = raw_data.copy()
            
            # Data cleaning and validation
            transformed_data = self._clean_claims_data(transformed_data)
            
            # Standardize procedure and diagnosis codes
            transformed_data = self._standardize_medical_codes(transformed_data)
            
            # Calculate derived fields
            transformed_data = self._calculate_derived_fields(transformed_data)
            
            # Apply business rules
            transformed_data = self._apply_business_rules(transformed_data)
            
            # Data quality validation
            self._validate_data_quality(transformed_data)
            
            logger.info(f"Transformation completed. Processed {len(transformed_data)} claims")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            raise
    
    def load_claims_data(self, transformed_data: pd.DataFrame) -> bool:
        """
        Load transformed claims data into Azure SQL database.
        
        Args:
            transformed_data: DataFrame containing transformed claims data
            
        Returns:
            True if load successful, False otherwise
        """
        logger.info("Starting data load to Azure SQL")
        
        try:
            # In real implementation, this would connect to Azure SQL
            # For demo purposes, we'll simulate the load
            batch_size = 1000
            total_records = len(transformed_data)
            
            for i in range(0, total_records, batch_size):
                batch = transformed_data.iloc[i:i+batch_size]
                self._load_batch_to_database(batch)
                self.processed_claims += len(batch)
                
                logger.info(f"Loaded batch {i//batch_size + 1}: {len(batch)} records")
            
            logger.info(f"Successfully loaded {self.processed_claims} claims to Azure SQL")
            return True
            
        except Exception as e:
            logger.error(f"Error during data load: {e}")
            self.failed_claims = len(transformed_data)
            return False
    
    def _generate_sample_claims_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Generate sample healthcare claims data for demonstration.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            
        Returns:
            DataFrame containing sample claims data
        """
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='H')
        num_records = len(date_range)
        
        # Sample procedure codes (CPT codes)
        procedure_codes = ['99213', '99214', '99215', '83036', '80053', '85025', '71020', '74177']
        
        # Sample diagnosis codes (ICD-10 codes)
        diagnosis_codes = ['I10', 'E11.9', 'J45.909', 'M54.5', 'R07.9', 'K21.9', 'F32.9', 'Z00.00']
        
        # Sample insurance types
        insurance_types = ['MEDICARE', 'MEDICAID', 'PRIVATE', 'SELF_PAY']
        
        # Sample claim statuses
        claim_statuses = ['PROCESSED', 'PENDING', 'REJECTED', 'APPROVED']
        
        # Generate random data
        np.random.seed(42)  # For reproducible results
        
        data = {
            'claim_id': [f'CLM{2024000 + i:06d}' for i in range(num_records)],
            'patient_id': [f'PAT{10000 + np.random.randint(0, 5000):05d}' for _ in range(num_records)],
            'provider_id': [f'PROV{100 + np.random.randint(0, 100):03d}' for _ in range(num_records)],
            'service_date': date_range,
            'procedure_code': np.random.choice(procedure_codes, num_records),
            'diagnosis_code': np.random.choice(diagnosis_codes, num_records),
            'amount_billed': np.random.uniform(50, 5000, num_records).round(2),
            'amount_paid': np.random.uniform(0, 4500, num_records).round(2),
            'insurance_type': np.random.choice(insurance_types, num_records),
            'claim_status': np.random.choice(claim_statuses, num_records, p=[0.6, 0.2, 0.1, 0.1]),
            'created_date': datetime.now() - timedelta(days=np.random.randint(1, 30)),
            'updated_date': datetime.now() - timedelta(hours=np.random.randint(1, 24))
        }
        
        return pd.DataFrame(data)
    
    def _clean_claims_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate claims data.
        
        Args:
            data: Raw claims DataFrame
            
        Returns:
            Cleaned claims DataFrame
        """
        # Remove duplicates
        initial_count = len(data)
        data = data.drop_duplicates(subset=['claim_id'])
        duplicates_removed = initial_count - len(data)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate claims")
        
        # Handle missing values
        data['amount_paid'] = data['amount_paid'].fillna(0)
        data['diagnosis_code'] = data['diagnosis_code'].fillna('UNKNOWN')
        
        # Validate data types
        data['service_date'] = pd.to_datetime(data['service_date'])
        data['amount_billed'] = pd.to_numeric(data['amount_billed'], errors='coerce')
        data['amount_paid'] = pd.to_numeric(data['amount_paid'], errors='coerce')
        
        # Remove invalid records
        data = data[data['amount_billed'] >= 0]
        data = data[data['amount_paid'] >= 0]
        
        logger.info(f"Data cleaning completed. {len(data)} valid records remain")
        return data
    
    def _standardize_medical_codes(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize medical procedure and diagnosis codes.
        
        Args:
            data: Claims DataFrame
            
        Returns:
            DataFrame with standardized codes
        """
        # Standardize procedure codes (remove dots, uppercase)
        data['procedure_code'] = data['procedure_code'].str.upper().str.replace('.', '')
        
        # Standardize diagnosis codes (uppercase)
        data['diagnosis_code'] = data['diagnosis_code'].str.upper()
        
        # Add code descriptions (simplified)
        procedure_descriptions = {
            '99213': 'Office visit',
            '99214': 'Office visit',
            '99215': 'Office visit',
            '83036': 'Hemoglobin A1c',
            '80053': 'Comprehensive metabolic panel',
            '85025': 'Complete blood count',
            '71020': 'Chest X-ray',
            '74177': 'CT abdomen'
        }
        
        data['procedure_description'] = data['procedure_code'].map(procedure_descriptions).fillna('Other procedure')
        
        return data
    
    def _calculate_derived_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived fields for analytics.
        
        Args:
            data: Claims DataFrame
            
        Returns:
            DataFrame with derived fields
        """
        # Calculate payment ratio
        data['payment_ratio'] = np.where(
            data['amount_billed'] > 0,
            data['amount_paid'] / data['amount_billed'],
            0
        )
        
        # Calculate days from service to claim
        data['days_to_claim'] = (data['created_date'] - data['service_date']).dt.days
        
        # Categorize claim amounts
        data['claim_category'] = pd.cut(
            data['amount_billed'],
            bins=[0, 100, 500, 2000, float('inf')],
            labels=['Small', 'Medium', 'Large', 'Extra Large']
        )
        
        # Generate unique claim hash for tracking
        data['claim_hash'] = data.apply(
            lambda row: hashlib.md5(
                f"{row['claim_id']}{row['patient_id']}{row['service_date']}".encode()
            ).hexdigest()[:16],
            axis=1
        )
        
        return data
    
    def _apply_business_rules(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply healthcare business rules and validation.
        
        Args:
            data: Claims DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Flag high-value claims for review
        data['high_value_flag'] = data['amount_billed'] > 10000
        
        # Flag unusual payment ratios
        data['unusual_payment_flag'] = (data['payment_ratio'] > 1.0) | (data['payment_ratio'] < 0.1)
        
        # Flag old claims (over 90 days)
        data['aged_claim_flag'] = data['days_to_claim'] > 90
        
        # Calculate risk score based on multiple factors
        data['risk_score'] = (
            (data['amount_billed'] / 1000) * 0.3 +
            (data['days_to_claim'] / 30) * 0.2 +
            (data['unusual_payment_flag'].astype(int) * 0.3) +
            (data['high_value_flag'].astype(int) * 0.2)
        ).round(2)
        
        return data
    
    def _validate_data_quality(self, data: pd.DataFrame) -> None:
        """
        Perform data quality validation checks.
        
        Args:
            data: Transformed claims DataFrame
            
        Raises:
            ValueError: If data quality issues are found
        """
        quality_issues = []
        
        # Check for missing critical fields
        missing_claims = data[data['claim_id'].isna()]
        if len(missing_claims) > 0:
            quality_issues.append(f"{len(missing_claims)} claims missing claim_id")
        
        # Check for negative amounts
        negative_billed = data[data['amount_billed'] < 0]
        if len(negative_billed) > 0:
            quality_issues.append(f"{len(negative_billed)} claims with negative billed amounts")
        
        # Check for future dates
        future_dates = data[data['service_date'] > datetime.now()]
        if len(future_dates) > 0:
            quality_issues.append(f"{len(future_dates)} claims with future service dates")
        
        # Log quality issues
        if quality_issues:
            logger.warning("Data quality issues found:")
            for issue in quality_issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("Data quality validation passed")
    
    def _load_batch_to_database(self, batch: pd.DataFrame) -> None:
        """
        Load a batch of data to the database.
        
        Args:
            batch: DataFrame containing batch of claims
        """
        # In real implementation, this would use SQLAlchemy or pyodbc
        # to insert data into Azure SQL database
        logger.debug(f"Loading batch of {len(batch)} claims to database")
        
        # Simulate database operation
        import time
        time.sleep(0.1)  # Simulate network latency
    
    def run_pipeline(self, start_date: datetime, end_date: datetime) -> Dict:
        """
        Run the complete ETL pipeline.
        
        Args:
            start_date: Start date for data processing
            end_date: End date for data processing
            
        Returns:
            Dictionary containing pipeline execution results
        """
        logger.info("Starting Healthcare Claims ETL Pipeline")
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = self.extract_claims_data(start_date, end_date)
            
            # Transform
            transformed_data = self.transform_claims_data(raw_data)
            
            # Load
            load_success = self.load_claims_data(transformed_data)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            results = {
                'status': 'SUCCESS' if load_success else 'FAILED',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'execution_time_seconds': execution_time,
                'records_processed': self.processed_claims,
                'records_failed': self.failed_claims,
                'total_records_extracted': len(raw_data),
                'total_records_transformed': len(transformed_data)
            }
            
            logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
            return results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            results = {
                'status': 'FAILED',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'execution_time_seconds': execution_time,
                'error': str(e),
                'records_processed': self.processed_claims,
                'records_failed': self.failed_claims
            }
            
            return results


def main():
    """
    Main function to run the ETL pipeline.
    """
    # Initialize pipeline
    pipeline = ClaimsETLPipeline()
    
    # Define date range for processing
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Process last 7 days
    
    # Run pipeline
    results = pipeline.run_pipeline(start_date, end_date)
    
    # Print results
    print("\n" + "="*50)
    print("HEALTHCARE CLAIMS ETL PIPELINE RESULTS")
    print("="*50)
    print(f"Status: {results['status']}")
    print(f"Execution Time: {results['execution_time_seconds']:.2f} seconds")
    print(f"Records Processed: {results['records_processed']}")
    print(f"Records Failed: {results['records_failed']}")
    
    if 'error' in results:
        print(f"Error: {results['error']}")
    
    print("="*50)


if __name__ == "__main__":
    main()
