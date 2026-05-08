# Healthcare Claims Processing ETL Pipeline

A comprehensive, beginner-friendly ETL (Extract, Transform, Load) pipeline for processing healthcare claims data from hospital databases to Azure SQL for analytics and reporting.

## 🏥 Project Overview

This project demonstrates modern data engineering practices in healthcare, implementing:
- **Automated CI/CD** with GitHub Actions
- **Comprehensive testing** with pytest
- **Code quality** with flake8 linting
- **Test coverage** reporting
- **Healthcare data standards** (CPT, ICD-10 codes)
- **Data validation** and business rules
- **Security best practices** for sensitive healthcare data

## 📋 Project Structure

```
healthcare-claims-etl/
├── .github/
│   └── workflows/
│       └── ci.yml                 # GitHub Actions CI/CD pipeline
├── src/
│   └── claims_etl.py             # Main ETL pipeline code
├── tests/
│   └── test_claims_etl.py        # Comprehensive test suite
├── data/
│   └── sample_claims.csv         # Sample healthcare claims data
├── config/
│   └── etl_config.yaml           # ETL configuration file
├── logs/                         # Log files (created automatically)
├── requirements.txt              # Python dependencies
└── README.md                     # This documentation
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or higher
- Git installed and configured
- GitHub account
- Basic understanding of healthcare claims (helpful but not required)

### Step 1: Create GitHub Repository

1. **Sign in to GitHub** at [github.com](https://github.com)
2. **Create a new repository:**
   - Click the "+" icon in the top right corner
   - Select "New repository"
   - Repository name: `healthcare-claims-etl`
   - Description: `Healthcare Claims Processing ETL Pipeline with CI/CD`
   - Choose "Public" or "Private" based on your needs
   - **Do NOT** initialize with README, .gitignore, or license
   - Click "Create repository"

### Step 2: Clone and Setup Locally

```bash
# Clone your new repository (replace YOUR_USERNAME with your GitHub username)
git clone https://github.com/YOUR_USERNAME/healthcare-claims-etl.git
cd healthcare-claims-etl

# Create the project structure
mkdir -p .github/workflows src tests data config logs

# Copy the project files (you'll create these from the templates above)
# Follow the file structure shown in the Project Structure section
```

### Step 3: Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### Step 4: Run the ETL Pipeline

```bash
# Run the main ETL pipeline
cd src
python claims_etl.py

# Run tests
cd ../tests
pytest test_claims_etl.py -v

# Run tests with coverage
pytest --cov=../src --cov-report=term-missing
```

### Step 5: Push to GitHub

```bash
# Configure Git (if not already done)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Add all files to Git
git add .

# Make initial commit
git commit -m "Initial commit: Healthcare Claims ETL Pipeline with CI/CD"

# Push to GitHub
git push origin main
```

## 🔧 GitHub Actions CI/CD Pipeline

### What the Pipeline Does

Our `.github/workflows/ci.yml` file automatically:

1. **Triggers on Pull Requests** - Runs every time someone creates or updates a PR
2. **Sets up Python 3.11** - Installs the correct Python version
3. **Installs Dependencies** - Installs all packages from `requirements.txt`
4. **Runs Linting** - Checks code style with flake8
5. **Runs Tests** - Executes all pytest tests
6. **Generates Coverage Report** - Measures test coverage
7. **Uploads Artifacts** - Saves coverage reports for review
8. **Security Scanning** - Checks for known vulnerabilities

### How to Validate the Workflow

1. **Create a Pull Request:**
   - Make a small change to any file
   - Commit and push the change
   - Create a pull request on GitHub

2. **Monitor the Actions:**
   - Go to the "Actions" tab in your GitHub repository
   - Click on the running workflow
   - Watch the progress in real-time

3. **Review Results:**
   - ✅ Green checkmark = All tests passed
   - ❌ Red X = Something failed (click for details)
   - Download coverage reports from the Artifacts section

## 📊 Understanding the ETL Pipeline

### Extract Phase
- **Source**: Hospital database (simulated)
- **Data**: Healthcare claims with patient, provider, and billing information
- **Format**: Structured data with standardized medical codes

### Transform Phase
- **Data Cleaning**: Remove duplicates, handle missing values
- **Standardization**: Normalize medical codes (CPT, ICD-10)
- **Business Rules**: Apply healthcare-specific validation
- **Derived Fields**: Calculate payment ratios, risk scores, aging metrics

### Load Phase
- **Target**: Azure SQL database (simulated)
- **Batch Processing**: Load data in manageable chunks
- **Error Handling**: Track successful vs. failed records
- **Audit Trail**: Maintain processing logs and metrics

## 🧪 Testing Strategy

### Test Coverage Areas

1. **Unit Tests**: Test individual functions and methods
2. **Integration Tests**: Test complete ETL workflow
3. **Data Validation Tests**: Verify data quality rules
4. **Error Handling Tests**: Ensure graceful failure handling
5. **Edge Case Tests**: Test unusual scenarios

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run tests with coverage
pytest --cov=src --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_claims_etl.py -v

# Run tests with specific pattern
pytest tests/ -k "test_pipeline" -v
```

## 🔍 Code Quality and Linting

### Why Linting Matters

- **Consistency**: Ensures all code follows the same style
- **Readability**: Makes code easier to understand and maintain
- **Error Prevention**: Catches potential bugs before they happen
- **Team Collaboration**: Reduces style debates in code reviews

### Linting Configuration

Our `flake8` configuration:
- **Max line length**: 88 characters (compatible with Black formatter)
- **Ignored codes**: E203, W503 (formatting conflicts)
- **Target directories**: `src/` and `tests/`

### Running Linting

```bash
# Run flake8 manually
flake8 src/ tests/ --max-line-length=88 --show-source --statistics

# Run with auto-fix (requires additional tools)
black src/ tests/
isort src/ tests/
```

## 🏥 Healthcare Data Standards

### Medical Codes Used

- **CPT Codes**: Current Procedural Terminology (procedures)
  - `99213-99215`: Office visits
  - `83036`: Hemoglobin A1c test
  - `80053`: Comprehensive metabolic panel
  - `85025`: Complete blood count
  - `71020`: Chest X-ray
  - `74177`: CT abdomen

- **ICD-10 Codes**: International Classification of Diseases (diagnoses)
  - `I10`: Hypertension
  - `E11.9`: Diabetes mellitus
  - `J45.909`: Asthma
  - `M54.5`: Low back pain
  - `R07.9`: Chest pain

### Data Privacy and Security

- **HIPAA Compliance**: All code designed with healthcare privacy in mind
- **Data Encryption**: Sensitive fields encrypted in transit and at rest
- **Access Control**: Role-based access to sensitive data
- **Audit Logging**: Complete audit trail of all data access

## 📈 Monitoring and Metrics

### Key Performance Indicators

1. **Processing Speed**: Records processed per second
2. **Data Quality**: Percentage of valid records
3. **Error Rate**: Failed vs. successful processing
4. **Coverage**: Test coverage percentage
5. **Pipeline Reliability**: Success/failure rates

### Monitoring Dashboard

The pipeline generates:
- **Execution Logs**: Detailed processing information
- **Quality Reports**: Data validation results
- **Performance Metrics**: Timing and resource usage
- **Error Summaries**: Failed processing details

## 🛠️ Advanced Configuration

### Environment-Specific Settings

The `config/etl_config.yaml` file supports multiple environments:

- **Development**: Local testing with relaxed validation
- **Staging**: Pre-production testing with production-like settings
- **Production**: Full security and validation enabled

### Custom Business Rules

Add your own healthcare business rules in the configuration:

```yaml
business_rules:
  custom_rules:
    - name: "pediatric_claims"
      condition: "patient_age < 18"
      action: "flag_for_review"
    - name: "emergency_claims"
      condition: "procedure_code in ['99281', '99282', '99283']"
      action: "priority_processing"
```

## 🤝 Contributing Guidelines

### How to Contribute

1. **Fork the Repository**
2. **Create Feature Branch**: `git checkout -b feature/new-feature`
3. **Make Changes**: Add your functionality
4. **Run Tests**: Ensure all tests pass
5. **Run Linting**: Fix any style issues
6. **Submit Pull Request**: Describe your changes

### Code Review Process

- **Automated Checks**: GitHub Actions runs automatically
- **Peer Review**: Team members review code changes
- **Testing Requirements**: Minimum 80% test coverage
- **Documentation**: Update docs for new features

## 📚 Learning Resources

### CI/CD Concepts

**What is CI/CD?**
- **Continuous Integration (CI)**: Automatically test and validate code changes
- **Continuous Deployment (CD)**: Automatically deploy validated code to production
- **Benefits**: Faster development, fewer bugs, better quality

**Why CI/CD Matters in Healthcare**
- **Patient Safety**: Automated testing reduces bugs that could affect patient care
- **Compliance**: Ensures code meets healthcare standards and regulations
- **Reliability**: Automated processes reduce human error
- **Audit Trail**: Complete history of all changes and deployments

### Testing in Healthcare Systems

**Why Automated Testing is Critical**
- **Patient Safety**: Bugs in healthcare systems can have serious consequences
- **Data Integrity**: Ensures patient data remains accurate and complete
- **Regulatory Compliance**: Many healthcare regulations require testing documentation
- **Risk Management**: Identifies issues before they affect patients

**Types of Testing**
- **Unit Tests**: Test individual components
- **Integration Tests**: Test how components work together
- **End-to-End Tests**: Test complete workflows
- **Performance Tests**: Ensure system can handle expected load

## 🔧 Troubleshooting

### Common Issues

1. **Python Version Errors**
   ```bash
   # Check Python version
   python --version
   # Should be 3.11 or higher
   ```

2. **Import Errors**
   ```bash
   # Ensure you're in the correct directory
   cd src
   python claims_etl.py
   ```

3. **Test Failures**
   ```bash
   # Run tests with verbose output
   pytest tests/ -v -s
   ```

4. **GitHub Actions Failures**
   - Check the "Actions" tab in GitHub
   - Review the error logs
   - Fix the issue and push a new commit

### Getting Help

1. **Check the Logs**: Look in the `logs/` directory for detailed error information
2. **Review Configuration**: Ensure `config/etl_config.yaml` is properly set up
3. **Validate Data**: Check that input data matches expected format
4. **Community Support**: Ask questions in GitHub Issues

## 📄 License and Legal

### Healthcare Data Considerations

- **HIPAA Compliance**: This project is designed for educational purposes
- **Sample Data**: All patient data is fictional and for demonstration only
- **Production Use**: Consult healthcare compliance experts before production deployment

### License

This project is provided for educational purposes. Please ensure compliance with:
- Healthcare regulations in your jurisdiction
- GitHub's terms of service
- Any applicable open source licenses

## 🎯 Next Steps

### Enhancements to Consider

1. **Real Database Integration**: Connect to actual hospital databases
2. **Advanced Analytics**: Add machine learning for fraud detection
3. **API Development**: Create REST APIs for data access
4. **Dashboard**: Build visualization dashboard for claims analytics
5. **Streaming**: Real-time claim processing with Apache Kafka

### Learning Path

1. **Master the Basics**: Understand the current pipeline completely
2. **Add Features**: Implement your own healthcare business rules
3. **Scale Up**: Handle larger datasets and optimize performance
4. **Productionize**: Add monitoring, alerting, and deployment automation

---

## 📞 Support

If you have questions or need help:
1. Check the troubleshooting section above
2. Review the GitHub Issues for similar problems
3. Create a new issue with detailed information about your problem

**Happy coding, and welcome to healthcare data engineering! 🏥💻**
