"""
Test Suite for Healthcare Claims Processing ETL Pipeline
========================================================

This module contains comprehensive unit tests for the ClaimsETLPipeline class.
Tests cover data extraction, transformation, loading, and error handling.

Author: Healthcare Data Team
Version: 1.0.0
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import yaml
from pathlib import Path

# Import the ETL pipeline module
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from claims_etl import ClaimsETLPipeline


class TestClaimsETLPipeline:
    """Test class for ClaimsETLPipeline functionality."""
    
    @pytest.fixture
    def sample_config(self):
        """Create a sample configuration for testing."""
        config = {
            'database': {
                'source': {
                    'connection_string': 'mock_connection_string'
                },
                'target': {
                    'connection_string': 'mock_target_connection_string'
                }
            },
            'etl': {
                'batch_size': 1000,
                'max_retries': 3
            },
            'validation': {
                'max_claim_amount': 100000,
                'min_claim_amount': 0
            }
        }
        return config
    
    @pytest.fixture
    def temp_config_file(self, sample_config):
        """Create a temporary configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        os.unlink(temp_file)
    
    @pytest.fixture
    def pipeline(self, temp_config_file):
        """Create a ClaimsETLPipeline instance for testing."""
        return ClaimsETLPipeline(config_path=temp_config_file)
    
    @pytest.fixture
    def sample_claims_data(self):
        """Create sample claims data for testing."""
        np.random.seed(42)  # For reproducible test data
        
        data = {
            'claim_id': [f'CLM{2024000 + i:06d}' for i in range(100)],
            'patient_id': [f'PAT{10000 + i:05d}' for i in range(100)],
            'provider_id': [f'PROV{100 + (i % 10):03d}' for i in range(100)],
            'service_date': pd.date_range(start='2024-01-01', periods=100, freq='D'),
            'procedure_code': ['99213', '99214', '83036', '80053'] * 25,
            'diagnosis_code': ['I10', 'E11.9', 'J45.909', 'M54.5'] * 25,
            'amount_billed': np.random.uniform(50, 5000, 100),
            'amount_paid': np.random.uniform(0, 4500, 100),
            'insurance_type': ['MEDICARE', 'MEDICAID', 'PRIVATE'] * 33 + ['SELF_PAY'],
            'claim_status': ['PROCESSED'] * 60 + ['PENDING'] * 20 + ['REJECTED'] * 20,
            'created_date': datetime.now() - timedelta(days=1),
            'updated_date': datetime.now()
        }
        
        return pd.DataFrame(data)
    
    def test_pipeline_initialization(self, temp_config_file):
        """Test that pipeline initializes correctly with valid config."""
        pipeline = ClaimsETLPipeline(config_path=temp_config_file)
        
        assert pipeline.config is not None
        assert pipeline.processed_claims == 0
        assert pipeline.failed_claims == 0
        assert 'database' in pipeline.config
    
    def test_pipeline_initialization_invalid_config(self):
        """Test that pipeline raises error with invalid config file."""
        with pytest.raises(FileNotFoundError):
            ClaimsETLPipeline(config_path='nonexistent_config.yaml')
    
    def test_load_config_success(self, temp_config_file):
        """Test successful configuration loading."""
        pipeline = ClaimsETLPipeline(config_path=temp_config_file)
        
        assert 'database' in pipeline.config
        assert 'etl' in pipeline.config
        assert pipeline.config['etl']['batch_size'] == 1000
    
    def test_generate_sample_claims_data(self, pipeline):
        """Test sample data generation."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)
        
        data = pipeline._generate_sample_claims_data(start_date, end_date)
        
        assert isinstance(data, pd.DataFrame)
        assert len(data) > 0
        assert 'claim_id' in data.columns
        assert 'patient_id' in data.columns
        assert 'procedure_code' in data.columns
        assert 'amount_billed' in data.columns
        
        # Check data types
        assert data['claim_id'].dtype == 'object'
        assert pd.api.types.is_numeric_dtype(data['amount_billed'])
        assert pd.api.types.is_datetime64_any_dtype(data['service_date'])
    
    def test_extract_claims_data(self, pipeline):
        """Test claims data extraction."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)
        
        extracted_data = pipeline.extract_claims_data(start_date, end_date)
        
        assert isinstance(extracted_data, pd.DataFrame)
        assert len(extracted_data) > 0
        assert all(col in extracted_data.columns for col in [
            'claim_id', 'patient_id', 'provider_id', 'service_date',
            'procedure_code', 'diagnosis_code', 'amount_billed', 'amount_paid'
        ])
    
    def test_clean_claims_data(self, pipeline, sample_claims_data):
        """Test data cleaning functionality."""
        # Add some dirty data
        dirty_data = sample_claims_data.copy()
        dirty_data.loc[0, 'claim_id'] = None  # Missing claim ID
        dirty_data.loc[1, 'amount_paid'] = None  # Missing amount
        dirty_data.loc[2, 'amount_billed'] = -100  # Negative amount
        
        # Add duplicate
        dirty_data = pd.concat([dirty_data, dirty_data.iloc[[0]]], ignore_index=True)
        
        cleaned_data = pipeline._clean_claims_data(dirty_data)
        
        # Should have fewer records after removing duplicates and invalid data
        assert len(cleaned_data) < len(dirty_data)
        
        # No missing claim IDs should remain
        assert cleaned_data['claim_id'].isna().sum() == 0
        
        # No negative amounts should remain
        assert (cleaned_data['amount_billed'] < 0).sum() == 0
    
    def test_standardize_medical_codes(self, pipeline, sample_claims_data):
        """Test medical code standardization."""
        # Add some non-standard codes
        test_data = sample_claims_data.copy()
        test_data.loc[0, 'procedure_code'] = '99213.1'  # With decimal
        test_data.loc[1, 'diagnosis_code'] = 'i10'  # Lowercase
        
        standardized_data = pipeline._standardize_medical_codes(test_data)
        
        # Procedure codes should be uppercase without decimals
        assert '992131' not in standardized_data['procedure_code'].values
        assert all(code.isupper() for code in standardized_data['procedure_code'].values)
        
        # Diagnosis codes should be uppercase
        assert all(code.isupper() for code in standardized_data['diagnosis_code'].values)
        
        # Procedure descriptions should be added
        assert 'procedure_description' in standardized_data.columns
    
    def test_calculate_derived_fields(self, pipeline, sample_claims_data):
        """Test calculation of derived fields."""
        transformed_data = pipeline._calculate_derived_fields(sample_claims_data)
        
        # Check new columns are added
        assert 'payment_ratio' in transformed_data.columns
        assert 'days_to_claim' in transformed_data.columns
        assert 'claim_category' in transformed_data.columns
        assert 'claim_hash' in transformed_data.columns
        
        # Payment ratio should be between 0 and 1 (or greater than 1 if overpaid)
        assert (transformed_data['payment_ratio'] >= 0).all()
        
        # Days to claim should be non-negative
        assert (transformed_data['days_to_claim'] >= 0).all()
        
        # Claim hash should be unique
        assert len(transformed_data['claim_hash'].unique()) == len(transformed_data)
    
    def test_apply_business_rules(self, pipeline, sample_claims_data):
        """Test business rules application."""
        # Add test data for business rules
        test_data = sample_claims_data.copy()
        test_data.loc[0, 'amount_billed'] = 15000  # High value claim
        test_data.loc[1, 'amount_paid'] = 6000  # Overpayment (ratio > 1)
        test_data.loc[2, 'days_to_claim'] = 100  # Aged claim
        
        processed_data = pipeline._apply_business_rules(test_data)
        
        # Check flag columns are added
        assert 'high_value_flag' in processed_data.columns
        assert 'unusual_payment_flag' in processed_data.columns
        assert 'aged_claim_flag' in processed_data.columns
        assert 'risk_score' in processed_data.columns
        
        # Check flags are set correctly
        assert processed_data.loc[0, 'high_value_flag'] == True
        assert processed_data.loc[1, 'unusual_payment_flag'] == True
        assert processed_data.loc[2, 'aged_claim_flag'] == True
        
        # Risk score should be numeric
        assert pd.api.types.is_numeric_dtype(processed_data['risk_score'])
    
    def test_validate_data_quality_success(self, pipeline, sample_claims_data):
        """Test data quality validation with good data."""
        # This should not raise an exception
        pipeline._validate_data_quality(sample_claims_data)
    
    def test_validate_data_quality_issues(self, pipeline, sample_claims_data):
        """Test data quality validation with problematic data."""
        # Add problematic data
        bad_data = sample_claims_data.copy()
        bad_data.loc[0, 'claim_id'] = None  # Missing claim ID
        bad_data.loc[1, 'amount_billed'] = -100  # Negative amount
        bad_data.loc[2, 'service_date'] = datetime.now() + timedelta(days=1)  # Future date
        
        # This should log warnings but not raise exceptions
        pipeline._validate_data_quality(bad_data)
    
    def test_transform_claims_data(self, pipeline, sample_claims_data):
        """Test complete data transformation process."""
        transformed_data = pipeline.transform_claims_data(sample_claims_data)
        
        # Should have all expected columns
        expected_columns = [
            'claim_id', 'patient_id', 'provider_id', 'service_date',
            'procedure_code', 'diagnosis_code', 'amount_billed', 'amount_paid',
            'payment_ratio', 'days_to_claim', 'claim_category', 'claim_hash',
            'high_value_flag', 'unusual_payment_flag', 'aged_claim_flag', 'risk_score'
        ]
        
        for col in expected_columns:
            assert col in transformed_data.columns
        
        # Data should be valid
        assert len(transformed_data) > 0
        assert transformed_data['claim_id'].isna().sum() == 0
    
    @patch('claims_etl.ClaimsETLPipeline._load_batch_to_database')
    def test_load_claims_data_success(self, mock_load_batch, pipeline, sample_claims_data):
        """Test successful data loading."""
        mock_load_batch.return_value = None
        
        result = pipeline.load_claims_data(sample_claims_data)
        
        assert result == True
        assert pipeline.processed_claims == len(sample_claims_data)
        assert pipeline.failed_claims == 0
        
        # Check that batch loading was called
        assert mock_load_batch.call_count > 0
    
    @patch('claims_etl.ClaimsETLPipeline._load_batch_to_database')
    def test_load_claims_data_failure(self, mock_load_batch, pipeline, sample_claims_data):
        """Test data loading failure."""
        mock_load_batch.side_effect = Exception("Database connection failed")
        
        result = pipeline.load_claims_data(sample_claims_data)
        
        assert result == False
        assert pipeline.failed_claims == len(sample_claims_data)
    
    def test_run_pipeline_success(self, pipeline):
        """Test complete pipeline execution."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)
        
        with patch.object(pipeline, 'load_claims_data', return_value=True):
            results = pipeline.run_pipeline(start_date, end_date)
        
        assert results['status'] == 'SUCCESS'
        assert 'execution_time_seconds' in results
        assert 'records_processed' in results
        assert 'total_records_extracted' in results
        assert 'total_records_transformed' in results
    
    def test_run_pipeline_failure(self, pipeline):
        """Test pipeline execution with failure."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)
        
        with patch.object(pipeline, 'extract_claims_data', side_effect=Exception("Extraction failed")):
            results = pipeline.run_pipeline(start_date, end_date)
        
        assert results['status'] == 'FAILED'
        assert 'error' in results
        assert 'execution_time_seconds' in results
    
    def test_payment_ratio_calculation(self, pipeline):
        """Test payment ratio calculation edge cases."""
        # Test data with edge cases
        test_data = pd.DataFrame({
            'amount_billed': [100, 0, 100],  # Normal, zero, normal
            'amount_paid': [80, 0, 120]      # Normal, zero, overpayment
        })
        
        result = pipeline._calculate_derived_fields(test_data)
        
        # Check payment ratios
        assert result.loc[0, 'payment_ratio'] == 0.8  # 80/100
        assert result.loc[1, 'payment_ratio'] == 0    # 0/0 should be 0
        assert result.loc[2, 'payment_ratio'] == 1.2  # 120/100
    
    def test_claim_hash_uniqueness(self, pipeline, sample_claims_data):
        """Test that claim hashes are unique."""
        transformed_data = pipeline._calculate_derived_fields(sample_claims_data)
        
        # All hashes should be unique
        hashes = transformed_data['claim_hash']
        assert len(hashes.unique()) == len(hashes)
        
        # Hashes should be consistent for same data
        hash1 = transformed_data.iloc[0]['claim_hash']
        hash2 = pipeline._calculate_derived_fields(sample_claims_data).iloc[0]['claim_hash']
        assert hash1 == hash2
    
    def test_risk_score_calculation(self, pipeline):
        """Test risk score calculation."""
        # Create test data with known risk factors
        test_data = pd.DataFrame({
            'amount_billed': [100, 5000, 15000],     # Small, medium, large
            'days_to_claim': [10, 50, 100],          # Normal, high, very high
            'unusual_payment_flag': [False, True, False],
            'high_value_flag': [False, False, True]
        })
        
        result = pipeline._apply_business_rules(test_data)
        
        # Risk scores should be calculated
        assert 'risk_score' in result.columns
        assert pd.api.types.is_numeric_dtype(result['risk_score'])
        
        # Higher risk factors should result in higher risk scores
        assert result.loc[2, 'risk_score'] > result.loc[0, 'risk_score']
    
    def test_batch_processing(self, pipeline, sample_claims_data):
        """Test that batch processing works correctly."""
        # Set small batch size for testing
        pipeline.config['etl']['batch_size'] = 10
        
        with patch.object(pipeline, '_load_batch_to_database') as mock_load:
            pipeline.load_claims_data(sample_claims_data)
        
        # Should be called multiple times for multiple batches
        assert mock_load.call_count > 1
        
        # Total processed should equal total records
        assert pipeline.processed_claims == len(sample_claims_data)


class TestDataValidation:
    """Test class specifically for data validation functionality."""
    
    @pytest.fixture
    def pipeline(self):
        """Create pipeline instance for validation tests."""
        # Create minimal config for testing
        config = {
            'database': {'source': {'connection_string': 'mock'}},
            'etl': {'batch_size': 1000},
            'validation': {'max_claim_amount': 100000}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            temp_file = f.name
        
        try:
            pipeline = ClaimsETLPipeline(config_path=temp_file)
            yield pipeline
        finally:
            os.unlink(temp_file)
    
    def test_empty_dataframe_handling(self, pipeline):
        """Test handling of empty dataframes."""
        empty_df = pd.DataFrame()
        
        # Should handle empty data gracefully
        result = pipeline._clean_claims_data(empty_df)
        assert len(result) == 0
    
    def test_single_record_processing(self, pipeline):
        """Test processing of single record."""
        single_record = pd.DataFrame({
            'claim_id': ['CLM000001'],
            'patient_id': ['PAT00001'],
            'amount_billed': [100.0],
            'amount_paid': [80.0]
        })
        
        result = pipeline._calculate_derived_fields(single_record)
        
        assert len(result) == 1
        assert 'payment_ratio' in result.columns
        assert result.loc[0, 'payment_ratio'] == 0.8


if __name__ == "__main__":
    # Run tests if this script is executed directly
    pytest.main([__file__, "-v"])
