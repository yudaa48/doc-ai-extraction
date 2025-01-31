# Document AI PDF Extraction

A Streamlit-based web application that leverages Google Cloud Document AI for processing PDF documents and extracting structured information. The application splits PDFs into pages, processes each page individually, and provides both page-wise and consolidated results in Excel format.

## Features

- PDF document upload and processing
- Page-by-page document analysis
- Entity extraction using Google Document AI
- Results visualization in the web interface
- Export to Excel with page-wise entity breakdown
- Automatic storage of results in Google Cloud Storage
- Progress tracking during document processing

## Prerequisites

### Google Cloud Setup

1. Create a Google Cloud Project
2. Enable the following APIs:
   - Document AI API
   - Cloud Storage API

3. Set up a Document AI processor
4. Create two Cloud Storage buckets:
   - One for input documents
   - One for processed results

5. Create a service account with the following roles:
   - `roles/documentai.apiUser`
   - `roles/storage.objectAdmin`
   - `roles/iam.serviceAccountUser`

### System Requirements

- Python 3.9 or higher
- poppler-utils (for PDF processing)
   ```bash
   sudo apt-get install poppler-utils  # For Ubuntu/Debian
   brew install poppler               # For macOS
   ```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd document-ai-extraction
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure the project settings in `main.py`:
   ```python
   PROJECT_CONFIG = {
       "project_id": "your-project-id",
       "location": "us",
       "processor_id": "your-processor-id",
       "input_bucket": "your-input-bucket",
       "output_bucket": "your-output-bucket"
   }
   ```

## Local Development

1. Set up Google Cloud authentication:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
   ```

2. Run the Streamlit application:
   ```bash
   streamlit run main.py
   ```

## Cloud Run Deployment

1. Build and deploy to Cloud Run:
   ```bash
   gcloud run deploy doc-ai \
     --source . \
     --platform managed \
     --region YOUR-REGION \
     --allow-unauthenticated \
     --timeout=300s \
     --cpu=1 \
     --memory=2Gi
   ```

## Project Structure

```
/document-ai-extraction
  ├── main.py              # Main application code
  ├── requirements.txt     # Python dependencies
  ├── Dockerfile          # Container configuration
  ├── .streamlit/         # Streamlit configuration
  │   └── config.toml     
  └── README.md           # Project documentation
```

## Usage

1. Access the web interface locally or via Cloud Run URL
2. Upload a PDF document using the file uploader
3. Click "Process Document" to start the analysis
4. View the extracted information for each page
5. Download the Excel file containing the processed results

## Code Components

### DocumentPageSplitter
Handles the splitting of PDF documents into individual pages for processing.

### DocumentAIProcessor
Main class that:
- Processes documents using Google Document AI
- Manages Google Cloud Storage operations
- Handles the conversion of results to Excel format

### Main Application
Streamlit interface that:
- Provides file upload functionality
- Shows processing progress
- Displays extracted information
- Offers download options for processed results

## Configuration

The application uses environment variables and a configuration dictionary:

```python
PROJECT_CONFIG = {
    "project_id": "your-project-id",
    "location": "us",              # Document AI processor location
    "processor_id": "processor-id", # Your Document AI processor ID
    "input_bucket": "input-bucket",
    "output_bucket": "output-bucket"
}
```

## Error Handling

The application includes comprehensive error handling for:
- File upload issues
- Processing failures
- Storage operations
- Missing configurations

## Support

For issues and feature requests, please create an issue in the repository.

## License

[Add your license information here]

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Authors

[Your Name/Organization]

## Acknowledgments

- Google Cloud Document AI
- Streamlit
- Python PDF processing libraries