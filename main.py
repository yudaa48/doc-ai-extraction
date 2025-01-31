import streamlit as st
import pandas as pd
import json
import os
import shutil
import PyPDF2
import io
from google.cloud import storage, documentai
from google.api_core.client_options import ClientOptions
from typing import Dict, Any, Optional, List
from datetime import datetime
from PIL import Image
from pdf2image import convert_from_path

# Predefined Configuration
PROJECT_CONFIG = {
    "project_id": "neon-camp-449123-j1",
    "location": "us",
    "processor_id": "cfebb242cf45e427",
    "input_bucket": "doc-ai-extraction",
    "output_bucket": "doc-ai-extraction"
}

class DocumentPageSplitter:
    def __init__(self, input_file_path: str):
        """
        Initialize page splitter for a given document
        
        Args:
            input_file_path (str): Path to the input document
        """
        self.input_file_path = input_file_path
        self.file_extension = os.path.splitext(input_file_path)[1].lower()
    
    def split_pdf_pages(self) -> List[str]:
        """
        Split PDF into individual page files
        
        Returns:
            List[str]: Paths to split page files
        """
        # Create output directory for page splits
        output_dir = 'page_splits'
        os.makedirs(output_dir, exist_ok=True)
        
        page_files = []
        
        # Open the PDF file
        with open(self.input_file_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            # Iterate through each page
            for page_num in range(len(pdf_reader.pages)):
                # Create a new PDF writer for this page
                pdf_writer = PyPDF2.PdfWriter()
                pdf_writer.add_page(pdf_reader.pages[page_num])
                
                # Generate output path for this page
                output_path = os.path.join(output_dir, f'page_{page_num + 1}.pdf')
                
                # Write the page to a new PDF file
                with open(output_path, 'wb') as output_file:
                    pdf_writer.write(output_file)
                
                page_files.append(output_path)
        
        return page_files

class DocumentAIProcessor:
    def __init__(self, project_id: str, location: str):
        """
        Initialize Document AI and Google Cloud Storage clients
        
        Args:
            project_id (str): Google Cloud Project ID
            location (str): Processor location (e.g., 'us', 'eu')
        """
        self.project_id = project_id
        self.location = location
        
        # Document AI client
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        self.documentai_client = documentai.DocumentProcessorServiceClient(client_options=opts)
        
        # Google Cloud Storage client
        self.storage_client = storage.Client()

    def upload_to_gcs(self, bucket_name: str, source_file_path: str, destination_blob_name: str, prefix: str = '') -> str:
        """
        Upload a file to Google Cloud Storage
        
        Args:
            bucket_name (str): Name of the GCS bucket
            source_file_path (str): Local path of the file to upload
            destination_blob_name (str): Destination filename
            prefix (str, optional): Prefix/folder path in the bucket
        
        Returns:
            str: GCS URI of the uploaded file
        """
        try:
            # Remove 'gs://' if present and split bucket path
            bucket_name = bucket_name.replace('gs://', '')
            # Split bucket path to handle nested paths
            bucket_parts = bucket_name.split('/')
            base_bucket = bucket_parts[0]
            
            # Add any additional path components to the prefix
            if len(bucket_parts) > 1:
                additional_path = '/'.join(bucket_parts[1:])
                prefix = f"{additional_path}/{prefix}" if prefix else additional_path
            
            # Verify bucket exists and is accessible
            bucket = self.storage_client.bucket(base_bucket)
            
            if not bucket.exists():
                raise ValueError(f"Bucket {base_bucket} does not exist or is not accessible")
            
            # Construct full blob path with prefix
            full_blob_path = f"{prefix}/{destination_blob_name}" if prefix else destination_blob_name
            full_blob_path = full_blob_path.replace('//', '/')  # Remove any double slashes
            
            # Upload the file
            blob = bucket.blob(full_blob_path)
            blob.upload_from_filename(source_file_path)
            
            return f"gs://{base_bucket}/{full_blob_path}"
        
        except Exception as e:
            st.error(f"GCS Upload Error: {str(e)}")
            raise

    def process_page(
        self, 
        processor_id: str, 
        file_path: str, 
        page_number: int
    ) -> Dict[str, Any]:
        """
        Process a single page document
        
        Args:
            processor_id (str): Document AI Processor ID
            file_path (str): Path to the page file
            page_number (int): Page number being processed
        
        Returns:
            dict: Processed page information
        """
        # Construct processor name
        name = self.documentai_client.processor_path(self.project_id, self.location, processor_id)
        
        # Read file
        with open(file_path, "rb") as pdf_file:
            pdf_content = pdf_file.read()
        
        # Prepare raw document
        raw_document = documentai.RawDocument(
            content=pdf_content, 
            mime_type="application/pdf"
        )
        
        # Process document
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        result = self.documentai_client.process_document(request=request)
        document = result.document
        
        # Enhance result with page number and location
        processed_page = self._document_to_dict(document)
        processed_page['page_number'] = page_number
        processed_page['original_file_path'] = file_path
        
        return processed_page

    def process_document_page_by_page(
        self, 
        input_file_path: str, 
        processor_id: str,
        cleanup: bool = True
    ) -> Dict[str, Any]:
        """
        Process document by splitting into pages and processing each
        
        Args:
            input_file_path (str): Path to input document
            processor_id (str): Document AI Processor ID
            cleanup (bool): Whether to remove temporary page files
        
        Returns:
            Dict with full document and page-wise processing results
        """
        # Step 1: Split document into pages
        page_splitter = DocumentPageSplitter(input_file_path)
        page_files = page_splitter.split_pdf_pages()
        
        # Prepare full document result
        full_document_result = {
            "pages": [],
            "page_texts": {},
            "entities": {},
            "text": ""
        }
        
        # Create progress bar
        progress_bar = st.progress(0)
        total_pages = len(page_files)
        
        # Step 2: Process each page
        for i, page_file in enumerate(page_files, 1):
            try:
                # Update progress bar
                progress_text = st.empty()
                progress_text.text(f"Processing page {i} of {total_pages}")
                progress_bar.progress(i / total_pages)
                
                # Process individual page
                processed_page = self.process_page(
                    processor_id=processor_id,
                    file_path=page_file,
                    page_number=i
                )
                
                # Accumulate full document text
                full_document_result["text"] += processed_page.get('text', '') + '\n'
                
                # Store page-specific text
                full_document_result["page_texts"][f"page_{i}"] = processed_page.get('text', '')
                
                # Collect page entities
                page_entities = processed_page.get('entities', {})
                for entity_type, entity_list in page_entities.items():
                    # Add page number to each entity
                    for entity in entity_list:
                        entity['pages'] = [i]
                    
                    # Accumulate global entities
                    if entity_type not in full_document_result["entities"]:
                        full_document_result["entities"][entity_type] = []
                    full_document_result["entities"][entity_type].extend(entity_list)
                
                # Store page info
                page_info = {
                    "page_number": i,
                    "entities": [
                        {
                            "type": entity_type, 
                            "value": entity['value'], 
                            "confidence": entity['confidence']
                        } for entity_type, entity_list in page_entities.items() 
                        for entity in entity_list
                    ]
                }
                full_document_result["pages"].append(page_info)
                
            except Exception as e:
                st.error(f"Error processing page {i}: {str(e)}")
            finally:
                # Clean up temporary page file
                if cleanup:
                    os.remove(page_file)
        
        # Clear progress indicators
        progress_bar.empty()
        progress_text.empty()
        
        return full_document_result

    def _document_to_dict(self, document) -> Dict[str, Any]:
        """
        Convert Document AI document to a dictionary with entities
        """
        document_dict = {
            "text": document.text,
            "entities": {}
        }
        
        # Process entities
        for entity in document.entities:
            entity_type = entity.type_.lower().replace(' ', '_')
            
            # Create entity info
            entity_info = {
                "type": entity_type,
                "value": entity.mention_text,
                "confidence": entity.confidence
            }
            
            # Store in entities
            if entity_type not in document_dict["entities"]:
                document_dict["entities"][entity_type] = []
            document_dict["entities"][entity_type].append(entity_info)
        
        return document_dict

    def save_json_to_gcs(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """
        Save JSON data to Google Cloud Storage
        """
        try:
            # Remove 'gs://' if present and split bucket path
            bucket_name = bucket_name.replace('gs://', '')
            bucket_parts = bucket_name.split('/')
            base_bucket = bucket_parts[0]
            
            # Add any additional path components to the prefix
            if len(bucket_parts) > 1:
                additional_path = '/'.join(bucket_parts[1:])
                prefix = f"{additional_path}/{prefix}" if prefix else additional_path
            
            bucket = self.storage_client.bucket(base_bucket)
            
            # Construct full blob path with prefix
            full_blob_path = f"{prefix}/{filename}" if prefix else filename
            full_blob_path = full_blob_path.replace('//', '/')  # Remove any double slashes
            
            blob = bucket.blob(full_blob_path)
            blob.upload_from_string(
                json.dumps(data, indent=2), 
                content_type='application/json'
            )
            return f"gs://{base_bucket}/{full_blob_path}"
        
        except Exception as e:
            st.error(f"JSON Save Error: {str(e)}")
            raise

    def save_excel_to_gcs(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """
        Convert JSON data to Excel with only page entities
        """
        try:
            # Remove 'gs://' if present and split bucket path
            bucket_name = bucket_name.replace('gs://', '')
            bucket_parts = bucket_name.split('/')
            base_bucket = bucket_parts[0]
            
            # Add any additional path components to the prefix
            if len(bucket_parts) > 1:
                additional_path = '/'.join(bucket_parts[1:])
                prefix = f"{additional_path}/{prefix}" if prefix else additional_path
            
            # Create Excel writer
            with pd.ExcelWriter('temp_output.xlsx', engine='xlsxwriter') as writer:
                # Process each page separately
                for page_data in data.get('pages', []):
                    page_num = page_data['page_number']
                    page_sheet_name = f'Page {page_num}'
                    
                    # Prepare multiple dataframes for the page sheet
                    workbook = writer.book
                    worksheet = workbook.add_worksheet(page_sheet_name)
                    
                    # Page Entities
                    if page_data.get('entities'):
                        worksheet.write(0, 0, 'Type')
                        worksheet.write(0, 1, 'Value')
                        worksheet.write(0, 2, 'Confidence')
                        
                        entity_row = 1
                        for entity in page_data['entities']:
                            worksheet.write(entity_row, 0, entity['type'])
                            worksheet.write(entity_row, 1, entity['value'])
                            worksheet.write(entity_row, 2, f"{entity['confidence']:.2%}")
                            entity_row += 1
                    else:
                        worksheet.write(0, 0, 'No entities found on this page')
            
            # Upload to GCS
            bucket = self.storage_client.bucket(base_bucket)
            
            # Construct full blob path with prefix
            full_blob_path = f"{prefix}/{filename}" if prefix else filename
            full_blob_path = full_blob_path.replace('//', '/')  # Remove any double slashes
            
            blob = bucket.blob(full_blob_path)
            blob.upload_from_filename('temp_output.xlsx')
            
            # Clean up local file
            os.remove('temp_output.xlsx')
            
            return f"gs://{base_bucket}/{full_blob_path}"
        
        except Exception as e:
            st.error(f"Excel Save Error: {str(e)}")
            raise

def download_file_from_gcs(bucket_name: str, source_blob_name: str) -> bytes:
    """
    Download a file from Google Cloud Storage
    
    Args:
        bucket_name (str): Name of the GCS bucket
        source_blob_name (str): Path to the file in the bucket
    
    Returns:
        bytes: File contents
    """
    # Remove 'gs://' if present
    bucket_name = bucket_name.replace('gs://', '')
    
    # Create storage client
    storage_client = storage.Client()
    
    # Get the bucket and blob
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    
    # Download the file
    return blob.download_as_bytes()

def main():
    st.title("Document AI PDF Extraction")
    
    # File upload
    uploaded_file = st.file_uploader("Choose a PDF document", type=['pdf'])
    
    # Initialize session state for document result and output files
    if 'document_result' not in st.session_state:
        st.session_state.document_result = None
    if 'excel_output_filename' not in st.session_state:
        st.session_state.excel_output_filename = None
    if 'output_bucket' not in st.session_state:
        st.session_state.output_bucket = PROJECT_CONFIG['output_bucket']
    
    # Process button
    if st.button("Process Document"):
        if uploaded_file is not None:
            try:
                # Use predefined configuration
                project_id = PROJECT_CONFIG['project_id']
                location = PROJECT_CONFIG['location']
                processor_id = PROJECT_CONFIG['processor_id']
                input_bucket = PROJECT_CONFIG['input_bucket']
                output_bucket = PROJECT_CONFIG['output_bucket']
                
                # Initialize processor
                processor = DocumentAIProcessor(project_id, location)
                
                # Generate unique filename preserving original name
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                original_filename = uploaded_file.name
                base_name, ext = os.path.splitext(original_filename)
                input_filename = f"{base_name}_{timestamp}{ext}"
                excel_output_filename = f"{base_name}_{timestamp}.xlsx"
                
                # Save uploaded file locally
                with open(input_filename, "wb") as f:
                    f.write(uploaded_file.getvalue())
                
                # Upload input file to GCS
                input_gcs_uri = processor.upload_to_gcs(
                    bucket_name=input_bucket,
                    source_file_path=input_filename, 
                    destination_blob_name=input_filename,
                    prefix="input"
                )
                st.success(f"Input file uploaded to: {input_gcs_uri}")
                
                # Process document page by page
                st.session_state.document_result = processor.process_document_page_by_page(
                    input_file_path=input_filename,
                    processor_id=processor_id
                )
                
                # Save Excel to GCS
                excel_gcs_uri = processor.save_excel_to_gcs(
                    bucket_name=output_bucket, 
                    data=st.session_state.document_result, 
                    filename=excel_output_filename,
                    prefix="output"
                )
                st.success(f"Excel results saved to: {excel_gcs_uri}")
                
                # Store filename for download
                st.session_state.excel_output_filename = f"output/{excel_output_filename}"
                
            except Exception as e:
                st.error(f"Error processing document: {str(e)}")
            
            # Clean up local file
            finally:
                if os.path.exists(input_filename):
                    os.remove(input_filename)
        else:
            st.warning("Please upload a PDF document")

    # Download button for processed Excel file
    if st.session_state.excel_output_filename:
        st.header("Download Processed File")
        
        # Excel download button
        try:
            # Download file from GCS
            excel_bytes = download_file_from_gcs(
                bucket_name=st.session_state.output_bucket, 
                source_blob_name=st.session_state.excel_output_filename
            )
            
            # Create download link
            st.download_button(
                label="Download Excel File",
                data=excel_bytes,
                file_name=os.path.basename(st.session_state.excel_output_filename),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Error downloading Excel file: {str(e)}")

    # Display results if available
    if st.session_state.document_result:
        st.header("Document Pages")
        
        # Display each page with its entities
        for page in st.session_state.document_result.get("pages", []):
            page_num = page["page_number"]
            with st.expander(f"Page {page_num}"):
                # Display page text
                st.subheader("Page Text")
                st.text_area(
                    f"Page {page_num} Text",
                    st.session_state.document_result["page_texts"].get(f"page_{page_num}", ""),
                    height=200
                )
                
                # Display page-specific entities
                st.subheader("Entities Found on This Page")
                if page["entities"]:
                    # Create DataFrame for entities on this page
                    entities_data = [{
                        "Entity Type": entity["type"],
                        "Value": entity["value"],
                        "Confidence": f"{entity['confidence']:.2%}"
                    } for entity in page["entities"]]
                    
                    # Sort entities by type for better organization
                    entities_df = pd.DataFrame(entities_data).sort_values("Entity Type")
                    
                    # Display as a table
                    st.dataframe(
                        entities_df,
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("No entities found on this page")

if __name__ == "__main__":
    main()

# Requirements (requirements.txt):
# streamlit
# google-cloud-documentai
# google-cloud-storage
# pandas
# xlsxwriter
# pdf2image
# pillow
# PyPDF2

# Note: 
# 1. Set up Google Cloud authentication before running
# 2. Install required libraries
# 3. Ensure you have the necessary Document AI processor and GCS buckets set up
# 4. Service account needs roles:
#    - roles/documentai.apiUser
#    - roles/storage.objectAdmin
#    - roles/iam.serviceAccountUser
# 5. System requirements:
#    - sudo apt-get install poppler-utils