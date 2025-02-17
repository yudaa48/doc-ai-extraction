import streamlit as st
import pandas as pd
import json
import os
import shutil
import PyPDF2
import io
import re
from google.cloud import storage, documentai
from google.api_core.client_options import ClientOptions
from typing import Dict, Any, Optional, List, Union, Union
from datetime import datetime
from PIL import Image
from pdf2image import convert_from_path

# Predefined Configuration
PROJECT_CONFIG = {
    "project_id": "neon-camp-449123-j1",
    "location": "us",
    "processor_id": "65b51dc1bf01ad16",
    "input_bucket": "doc-ai-extraction",
    "output_bucket": "doc-ai-extraction"
}

class DocumentPageSplitter:
    def __init__(self, input_file_path: str, output_dir: str = 'page_splits'):
        """
        Initialize page splitter for a given document
        
        Args:
            input_file_path (str): Path to the input document
            output_dir (str, optional): Directory to save split pages. Defaults to 'page_splits'.
        """
        self.input_file_path = input_file_path
        self.file_extension = os.path.splitext(input_file_path)[1].lower()
        
        # Ensure output directory exists
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def split_pdf_pages(self) -> List[str]:
        """
        Split PDF into individual page files
        
        Returns:
            List[str]: Paths to split page files
        
        Raises:
            ValueError: If input file is not a PDF
            FileNotFoundError: If input file does not exist
        """
        # Validate input file
        if not os.path.exists(self.input_file_path):
            raise FileNotFoundError(f"Input file not found: {self.input_file_path}")
        
        if self.file_extension != '.pdf':
            raise ValueError(f"Unsupported file type. Expected PDF, got {self.file_extension}")
        
        page_files = []
        
        # Open the PDF file
        try:
            with open(self.input_file_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                
                # Iterate through each page
                for page_num in range(len(pdf_reader.pages)):
                    # Create a new PDF writer for this page
                    pdf_writer = PyPDF2.PdfWriter()
                    pdf_writer.add_page(pdf_reader.pages[page_num])
                    
                    # Generate output path for this page
                    output_path = os.path.join(
                        self.output_dir, 
                        f'page_{page_num + 1}.pdf'
                    )
                    
                    # Write the page to a new PDF file
                    with open(output_path, 'wb') as output_file:
                        pdf_writer.write(output_file)
                    
                    page_files.append(output_path)
        
        except Exception as e:
            # Log the specific error
            print(f"Error splitting PDF: {e}")
            # Clean up any partially created files
            for file in page_files:
                try:
                    os.remove(file)
                except:
                    pass
            raise
        
        # Verify files were created
        if not page_files:
            raise ValueError("No pages were extracted from the PDF")
        
        return page_files

    def cleanup(self):
        """
        Remove the temporary page split directory
        """
        try:
            if os.path.exists(self.output_dir):
                shutil.rmtree(self.output_dir)
        except Exception as e:
            print(f"Error cleaning up page splits: {e}")

class DocumentAIProcessor:
    def __init__(self, project_id: str, location: str):
        """Initialize Document AI and Google Cloud Storage clients"""
        self.project_id = project_id
        self.location = location
        
        # Document AI client
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        self.documentai_client = documentai.DocumentProcessorServiceClient(client_options=opts)
        
        # Google Cloud Storage client
        self.storage_client = storage.Client()
        
        # Define main sections based on schema
        self.main_sections = [
            'charges', 'cmv', 'damage', 'disposition_of_injured_killed',
            'factors_conditions', 'identification_location', 'investigator',
            'narrative', 'vehicle_driver_persons'
        ]

    def process_page(
        self, 
        processor_id: str, 
        file_path: str, 
        page_number: int
    ) -> Dict[str, Any]:
        """
        Process a single page document
        """
        # Construct processor name
        name = self.documentai_client.processor_path(self.project_id, self.location, processor_id)
        
        # Read file
        with open(file_path, "rb") as pdf_file:
            pdf_content = pdf_file.read()
            print(f"Page {page_number} content size: {len(pdf_content)} bytes")
        
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
        
        try:
            result = self.documentai_client.process_document(request=request)
            document = result.document
            
            # Log document entities count
            print(f"Page {page_number} found {len(document.entities)} entities")
            
            # Convert to dictionary with debug information
            processed_page = self._document_to_dict(document, page_number)
            processed_page['page_number'] = page_number
            processed_page['original_file_path'] = file_path
            
            return processed_page
            
        except Exception as e:
            print(f"Error processing page {page_number}: {str(e)}")
            raise

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

    def _document_to_dict(self, document, page_number=None) -> Dict[str, Any]:
        """
        Convert Document AI document to dictionary with section-based organization
        
        Args:
            document: Document AI document object
            page_number: Optional page number for logging
            
        Returns:
            Dict containing structured document data
        """
        document_dict = {
            "text": document.text,
            "sections": {section: [] for section in self.main_sections}
        }
        
        # Create entity mapping
        entity_map = {}
        
        # First pass: Create all entities and store in map
        for entity in document.entities:
            entity_id = id(entity)
            print(f"\nProcessing entity: {entity.type_}")
            
            # Get the base section type
            section = self._get_entity_section(entity.type_)
            if not section:
                print(f"No section found for entity type: {entity.type_}")
                continue
                
            entity_info = {
                "type": entity.type_.lower().replace(' ', '_'),
                "value": entity.mention_text,
                "confidence": entity.confidence,
                "child_entities": [],
                "parent_id": None,
                "section": section  # Store section information
            }
            
            # Add normalized value if it exists
            if hasattr(entity, 'normalized_value'):
                if isinstance(entity.normalized_value, dict):
                    entity_info["normalized_value"] = entity.normalized_value.get('text', '')
                else:
                    entity_info["normalized_value"] = str(entity.normalized_value)
            
            # Process properties (child entities)
            if hasattr(entity, 'properties') and entity.properties:
                print(f"Found {len(entity.properties)} child properties")
                for child in entity.properties:
                    child_id = id(child)
                    child_info = {
                        "type": child.type_.lower().replace(' ', '_'),
                        "value": child.mention_text,
                        "confidence": child.confidence,
                        "child_entities": [],
                        "parent_id": entity_id
                    }
                    
                    # Process subproperties (grandchild entities)
                    if hasattr(child, 'properties') and child.properties:
                        print(f"Found {len(child.properties)} grandchild properties")
                        for subchild in child.properties:
                            subchild_id = id(subchild)
                            subchild_info = {
                                "type": subchild.type_.lower().replace(' ', '_'),
                                "value": subchild.mention_text,
                                "confidence": subchild.confidence,
                                "parent_id": child_id
                            }
                            child_info["child_entities"].append(subchild_info)
                    
                    entity_info["child_entities"].append(child_info)
            
            entity_map[entity_id] = entity_info
            
            # Add to appropriate section
            document_dict["sections"][section].append(entity_info)
        
        return document_dict

    def _get_entity_section(self, entity_type: str) -> Optional[str]:
        """Determine which main section an entity belongs to, supporting nested types"""
        entity_type_lower = entity_type.lower()
        
        # Check direct section match
        for section in self.main_sections:
            if entity_type_lower.startswith(section):
                return section
                
        # Check nested type patterns
        if '/' in entity_type_lower:
            base_type = entity_type_lower.split('/')[0]
            for section in self.main_sections:
                if base_type.startswith(section):
                    return section
        
        return None

    def save_json_to_gcs(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """Save JSON data to Google Cloud Storage with section-based organization"""
        try:
            bucket_name = bucket_name.replace('gs://', '')
            bucket_parts = bucket_name.split('/')
            base_bucket = bucket_parts[0]
            
            if len(bucket_parts) > 1:
                additional_path = '/'.join(bucket_parts[1:])
                prefix = f"{additional_path}/{prefix}" if prefix else additional_path
            
            bucket = self.storage_client.bucket(base_bucket)
            full_blob_path = f"{prefix}/{filename}" if prefix else filename
            full_blob_path = full_blob_path.replace('//', '/')
            
            # Organize data by sections
            organized_data = {
                "document_text": data.get("text", ""),
                "sections": data.get("sections", {})
            }
            
            blob = bucket.blob(full_blob_path)
            blob.upload_from_string(
                json.dumps(organized_data, indent=2),
                content_type='application/json'
            )
            return f"gs://{base_bucket}/{full_blob_path}"
            
        except Exception as e:
            st.error(f"JSON Save Error: {str(e)}")
            raise
    
    def _process_person_details_for_excel(self, page_num, person_entries) -> List[Dict]:
        """
        Process person details for Excel output with expanded fields and decoded values
        
        Args:
            page_num (int): Page number
            person_entries (List[Dict]): List of person entries
        
        Returns:
            List of processed rows for Excel output
        """
        rows = []
        
        for person_entry in person_entries:
            # Create a processor to handle detailed description parsing
            processor = CrashReportDataProcessor()
            
            # Process each entity in the person entry
            basic_info = {
                'name': '',
                'type': '',
                'person_num1': '',
                'seat_position': ''
            }
            description_details = None
            
            for entity in person_entry.get('entities', []):
                # Basic person information
                if entity['type'] == 'person_name':
                    basic_info['name'] = entity['value']
                
                if entity['type'] == 'person_type':
                    # Get both original and decoded values
                    original_type = entity['value']
                    basic_info['type'] = {
                        'original': original_type,
                        'decoded': processor.data_dict.get_description('PERSON_TYPE', original_type)
                    }
                
                if entity['type'] == 'person_num1':
                    basic_info['person_num1'] = entity['value']
                
                if entity['type'] == 'person_seat_position':
                    basic_info['seat_position'] = entity['value']
                
                # Detailed person description processing
                if entity['type'] == 'person_description':
                    description_details = processor.extract_person_description(entity['value'])
            
            # Create rows for basic information
            basic_rows = []
            for key, value in basic_info.items():
                # Special handling for type to show both original and decoded
                if key == 'type' and isinstance(value, dict):
                    basic_rows.append({
                        "Page": page_num,
                        "Level": "Person",
                        "Type": "Person Type",
                        "Value": value['original'],
                        "Decoded Value": value['decoded'],
                        "Raw Value": "",
                        "Confidence": f"{entity.get('confidence', 0):.2%}"
                    })
                else:
                    basic_rows.append({
                        "Page": page_num,
                        "Level": "Person",
                        "Type": key.replace('_', ' ').title(),
                        "Value": value,
                        "Decoded Value": "",
                        "Raw Value": "",
                        "Confidence": f"{entity.get('confidence', 0):.2%}"
                    })
            rows.extend(basic_rows)
            
            # Process detailed description if available
            if description_details:
                # Mapping of description fields to more readable names and decode methods
                description_field_mappings = {
                    'injury_severity': ('INJURY_SEVERITY', 'Injury Severity'),
                    'age': (None, 'Age'),
                    'ethnicity': ('ETHNICITY', 'Ethnicity'),
                    'sex': ('SEX_CODES', 'Sex'),
                    'eject': ('EJECT_CODES', 'Eject Status'),
                    'restr': ('RESTRAINT_CODES', 'Restraint'),
                    'airbag': ('AIRBAG_CODES', 'Airbag Status'),
                    'helmet': ('HELMET_CODES', 'Helmet Status'),
                    'sol': ('SOBRIETY_CODES', 'Sobriety of Last Drink'),
                    'alc_spec': ('SUBSTANCE_SPEC_CODES', 'Alcohol Specification'),
                    'alc_result': ('SUBSTANCE_RESULT_CODES', 'Alcohol Result'),
                    'drug_spec': ('SUBSTANCE_SPEC_CODES', 'Drug Specification'),
                    'drug_result': ('SUBSTANCE_RESULT_CODES', 'Drug Result'),
                    'drug_category': ('SUBSTANCE_SPEC_CODES', 'Drug Category')
                }
                
                # Create rows for each description detail
                description_rows = []
                for key, value in description_details.items():
                    # Get decode method and display name
                    decode_method, display_name = description_field_mappings.get(key, (None, key.replace('_', ' ').title()))
                    
                    # Decode if possible
                    decoded_value = ''
                    if decode_method and hasattr(processor.data_dict, decode_method):
                        code_dict = getattr(processor.data_dict, decode_method)
                        decoded_value = code_dict.get(str(value), value)
                    
                    description_rows.append({
                        "Page": page_num,
                        "Level": "Person Description",
                        "Type": display_name,
                        "Value": value,
                        "Decoded Value": decoded_value,
                        "Raw Value": "",
                        "Confidence": ""
                    })
                
                rows.extend(description_rows)
            
            # Add a separator row
            rows.append({
                "Page": page_num,
                "Level": "Separator",
                "Type": "",
                "Value": "",
                "Decoded Value": "",
                "Raw Value": "",
                "Confidence": ""
            })
        
        return rows

    def save_excel_to_gcs(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """Save crash report data to Excel with enhanced processing"""
        try:
            # Initialize processors
            processor = CrashReportDataProcessor()
            checkbox_processor = CheckboxProcessor()
            section_unique_trackers = {}
            
            # Create Excel writer with xlsxwriter engine
            with pd.ExcelWriter('temp_output.xlsx', engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # Process each page
                for page in data.get("pages", []):
                    page_num = page["page_number"]
                    hierarchical_fields = page.get("hierarchical_fields", {})
                    
                    # Process sections in a specific order
                    section_order = [
                        'identification_location', 
                        'vehicle_driver_persons', 
                        'factors_conditions', 
                        'charges', 
                        'damage', 
                        'disposition_of_injured_killed', 
                        'investigator', 
                        'narrative'
                    ]
                    
                    for section_type in section_order:
                        section_data = hierarchical_fields.get(section_type, [])
                        
                        if not section_data:
                            continue
                        
                        # Increment section tracker
                        if section_type not in section_unique_trackers:
                            section_unique_trackers[section_type] = 0
                        section_unique_trackers[section_type] += 1
                        
                        # Generate sheet name
                        sheet_name = f"P{page_num}_{section_type}_{section_unique_trackers[section_type]}"[:31]
                        rows = []
                        
                        # Specific processing for each section type
                        if section_type == 'identification_location':
                            for location in section_data:
                                # Process each child field
                                for field_type, field_entries in location.get("child_fields", {}).items():
                                    for entry in field_entries:
                                        # Process checkbox values
                                        processed_value = checkbox_processor.process_json_field(entry)
                                        
                                        rows.append({
                                            "Page": page_num,
                                            "Level": "Field",
                                            "Type": field_type,
                                            "Value": processed_value.get('value', ''),
                                            "Raw Value": processed_value.get('raw_value', ''),
                                            "Confidence": f"{processed_value.get('confidence', 0):.2%}"
                                        })
                        
                        elif section_type == 'vehicle_driver_persons':
                            # Process person details
                            for parent_idx, parent_entity in enumerate(section_data, 1):
                                # Add parent information
                                parent_proc = checkbox_processor.process_json_field(parent_entity)
                                rows.append({
                                    "Page": page_num,
                                    "Level": "Parent",
                                    "Type": parent_proc.get('type', ''),
                                    "Value": parent_proc.get('value', ''),
                                    "Raw Value": parent_proc.get('raw_value', ''),
                                    "Confidence": f"{parent_proc.get('confidence', 0):.2%}"
                                })
                                
                                # Process child fields
                                for child_type, child_entries in parent_entity.get("child_fields", {}).items():
                                    if child_type == 'person_num':
                                        for person_idx, child_entry in enumerate(child_entries, 1):
                                            # Add person header
                                            rows.append({
                                                "Page": page_num,
                                                "Level": "Person Header",
                                                "Type": f"Person {person_idx}",
                                                "Value": f"Person {person_idx} Details",
                                                "Raw Value": "",
                                                "Confidence": ""
                                            })
                                            
                                            # Process person entities
                                            for entity in child_entry.get("entities", []):
                                                proc_entity = checkbox_processor.process_json_field(entity)
                                                rows.append({
                                                    "Page": page_num,
                                                    "Level": "Entity",
                                                    "Type": proc_entity.get('type', ''),
                                                    "Value": proc_entity.get('value', ''),
                                                    "Raw Value": proc_entity.get('raw_value', ''),
                                                    "Confidence": f"{proc_entity.get('confidence', 0):.2%}"
                                                })
                                    
                                    else:
                                        for child_entry in child_entries:
                                            proc_child = checkbox_processor.process_json_field(child_entry)
                                            rows.append({
                                                "Page": page_num,
                                                "Level": "Child",
                                                "Type": child_type,
                                                "Value": proc_child.get('value', ''),
                                                "Raw Value": proc_child.get('raw_value', ''),
                                                "Confidence": f"{proc_child.get('confidence', 0):.2%}"
                                            })
                        
                        elif section_type == 'factors_conditions':
                            for entity in section_data:
                                # Add overall section entity
                                parent_proc = checkbox_processor.process_json_field(entity)
                                rows.append({
                                    "Page": page_num,
                                    "Level": "Parent",
                                    "Type": section_type,
                                    "Value": parent_proc.get('value', ''),
                                    "Raw Value": parent_proc.get('raw_value', ''),
                                    "Confidence": f"{parent_proc.get('confidence', 0):.2%}"
                                })
                                
                                # Process child fields
                                for child_type, child_entries in entity.get("child_fields", {}).items():
                                    for child_entry in child_entries:
                                        proc_child = checkbox_processor.process_json_field(child_entry)
                                        rows.append({
                                            "Page": page_num,
                                            "Level": "Child",
                                            "Type": child_type,
                                            "Value": proc_child.get('value', ''),
                                            "Raw Value": proc_child.get('raw_value', ''),
                                            "Confidence": f"{proc_child.get('confidence', 0):.2%}"
                                        })
                        
                        # Generic processing for other sections
                        else:
                            for entity in section_data:
                                # Process entity
                                proc_entity = checkbox_processor.process_json_field(entity)
                                rows.append({
                                    "Page": page_num,
                                    "Level": "Parent",
                                    "Type": section_type,
                                    "Value": proc_entity.get('value', ''),
                                    "Raw Value": proc_entity.get('raw_value', ''),
                                    "Confidence": f"{proc_entity.get('confidence', 0):.2%}"
                                })
                                
                                # Process child fields
                                for child_type, child_entries in entity.get("child_fields", {}).items():
                                    for child_entry in child_entries:
                                        proc_child = checkbox_processor.process_json_field(child_entry)
                                        rows.append({
                                            "Page": page_num,
                                            "Level": "Child",
                                            "Type": child_type,
                                            "Value": proc_child.get('value', ''),
                                            "Raw Value": proc_child.get('raw_value', ''),
                                            "Confidence": f"{proc_child.get('confidence', 0):.2%}"
                                        })
                        
                        # Create DataFrame and write to Excel
                        if rows:
                            df_section = pd.DataFrame(rows)
                            df_section.to_excel(writer, sheet_name=sheet_name, index=False)
                            
                            # Format worksheet
                            worksheet = writer.sheets[sheet_name]
                            header_format = workbook.add_format({
                                'bold': True,
                                'bg_color': '#D3D3D3',
                                'align': 'center'
                            })
                            
                            # Apply formats
                            for row_idx, row in enumerate(rows, 1):
                                if row.get('Level') in ['Section Header', 'Parent', 'Person Header']:
                                    worksheet.set_row(row_idx, None, header_format)
                            
                            self._adjust_column_widths(writer, sheet_name, df_section)
            
            # Upload to GCS
            bucket_name = bucket_name.replace('gs://', '')
            bucket = self.storage_client.bucket(bucket_name)
            
            full_blob_path = f"{prefix}/{filename}" if prefix else filename
            full_blob_path = full_blob_path.replace('//', '/')
            
            blob = bucket.blob(full_blob_path)
            blob.upload_from_filename('temp_output.xlsx')
            
            # Clean up temporary file
            os.remove('temp_output.xlsx')
            
            return f"gs://{bucket_name}/{full_blob_path}"
            
        except Exception as e:
            st.error(f"Excel Save Error: {str(e)}")
            # Clean up temp file if it exists
            if os.path.exists('temp_output.xlsx'):
                os.remove('temp_output.xlsx')
            raise

    def _format_worksheet(self, worksheet, df, header_format, cell_format):
        """Apply formatting to worksheet"""
        # Set column widths
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            ) + 2
            worksheet.set_column(idx, idx, min(max_length, 50))
        
        # Format headers
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Format cells
        for row_num in range(len(df)):
            for col_num in range(len(df.columns)):
                worksheet.write(row_num + 1, col_num, df.iloc[row_num, col_num], cell_format)

    def _process_section_data(self, section_name: str, entities: List[Dict], fields: Union[List[str], Dict]) -> pd.DataFrame:
        """
        Process section data into a DataFrame with hierarchical structure
        
        Args:
            section_name (str): Name of the section being processed
            entities (List[Dict]): List of entities from the document
            fields (Union[List[str], Dict]): Hierarchy of fields to extract
        
        Returns:
            pd.DataFrame: Processed entities with hierarchical information
        """
        def _extract_hierarchical_entities(
            current_entities: List[Dict], 
            current_fields: Union[List[str], Dict], 
            parent_field: str = ''
        ) -> List[Dict]:
            """
            Recursively extract entities based on the field hierarchy
            
            Args:
                current_entities (List[Dict]): Entities to process
                current_fields (Union[List[str], Dict]): Fields to extract
                parent_field (str, optional): Parent field name
            
            Returns:
                List[Dict]: Extracted hierarchical entities
            """
            extracted_entities = []
            
            # Handle list of simple fields
            if isinstance(current_fields, list):
                for entity in current_entities:
                    entity_type = str(entity.get("type", "")).lower()
                    
                    # Check if entity type matches any of the fields
                    if entity_type in [str(f).lower() for f in current_fields]:
                        extracted_entities.append({
                            "parent_field": parent_field,
                            "type": entity_type,
                            "value": str(entity.get("value", "")),
                            "confidence": f"{entity.get('confidence', 0):.2%}",
                            "page_number": entity.get("page_number", "")
                        })
            
            # Handle nested dictionary structure
            elif isinstance(current_fields, dict):
                for field, subfields in current_fields.items():
                    # Convert field to string and lowercase
                    field_str = str(field).lower()
                    
                    # Find entities matching the current field
                    matching_entities = [
                        e for e in current_entities 
                        if str(e.get("type", "")).lower() == field_str
                    ]
                    
                    # Add parent field entry if matching entities exist
                    if matching_entities:
                        # Add the parent field entry
                        extracted_entities.append({
                            "parent_field": parent_field,
                            "type": field_str,
                            "value": "",
                            "confidence": f"{matching_entities[0].get('confidence', 0):.2%}",
                            "page_number": matching_entities[0].get("page_number", "")
                        })
                        
                        # Recursively process subfields
                        extracted_entities.extend(
                            _extract_hierarchical_entities(
                                matching_entities, 
                                subfields, 
                                parent_field=field_str
                            )
                        )
            
            return extracted_entities
        
        # Main processing: extract entities based on fields
        try:
            processed_rows = _extract_hierarchical_entities(entities, fields)
            
            # Convert to DataFrame, return empty DataFrame if no rows
            return pd.DataFrame(processed_rows) if processed_rows else pd.DataFrame()
        
        except Exception as e:
            # Log any unexpected errors during processing
            print(f"Error processing section {section_name}: {e}")
            return pd.DataFrame()

    def _adjust_column_widths(self, writer, sheet_name, df):
        """Adjust column widths in Excel worksheet"""
        worksheet = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns):
            # Get maximum length of column items
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            ) + 2
            worksheet.set_column(idx, idx, min(max_length, 50))  # Cap width at 50

    def _get_field_hierarchy(self) -> Dict[str, Union[List[str], Dict]]:
        """Define the hierarchical structure of fields"""
        return {
            'charges': {
                'unit_num': ['charge', 'citation_ref_num', 'person_num_charges', 'unit_num_charges']
            },
            'cmv': [
                'actual_gross_weight', 'bus_type_cmv', 'capacity_cmv', 'cargo_body_type',
                'carrier_corp_name', 'carrier_id_num', 'carrier_id_type_cmv', 
                'carrier_primary_address', 'cmv_disabling_damage', 'disabling_damage_cmv',
                'hazmat_class_num', 'hazmat_id_num', 'hazmat_released', 'intermodal_shipping',
                'lbs_cmv', 'rgvw_gvwr', 'rgvw_gvwr_tick_box', 'sequence_event_1',
                'sequence_event_2', 'sequence_event_3', 'sequence_event_4', 'total_num_axies',
                'transporting_hazardous_material_cmv', 'trlr_type', 'unit_num_cmv',
                'veh_oper_cmv', 'veh_type_cmv'
            ],
            'damage': {
                'damaged_property_other_than_vehicle': [
                    'damaged_property', 'owner_address_damage', 'owner_name_damage'
                ]
            },
            'disposition_of_injured_killed': {
                'unit_num_disposition': [
                    'date_of_death', 'person_num_disposition', 'taken_by', 'taken_to',
                    'time_of_death', 'unit_num_disp'
                ]
            },
            'factors_conditions': {
                'unit_contributing_factors': [
                    'contributing_contributing_factors', 'contributing_vehicle_defects',
                    'entering_roads', 'light_cond', 'may_have_contrib_vehicle_defects',
                    'may_have_contributing_factors', 'roadway_alignment', 'roadway_type',
                    'surface_condition', 'traffic_control', 'unit_num_contributing',
                    'weather_cond'
                ]
            },
            'identification_location': [
                'block_num', 'case_id', 'city_name', 'const_zone', 'country_name',
                'crash_damage_1000', 'crash_date', 'crash_time', 'dir_from_int_or_ref_marker',
                'dir_of_traffic', 'distance_from_int_of_ref_marker', 'hwy_num', 'latitude',
                'local_use', 'longitude', 'outside_city_limit', 'rdwy_part', 'rdwy_sys',
                'ref_marker', 'rrx_num', 'speed_limit', 'street_desc', 'street_name',
                'street_prefix', 'street_suffix', 'worker_present'
            ],
            'investigator': [
                'agency_name', 'date_arrived', 'date_notified', 'date_roadway',
                'date_scene_cleared', 'how_notified', 'id_num_investigator',
                'investigation_complete', 'investigator_name', 'ori_num', 'report_date',
                'service_region_da', 'time_arrived', 'time_notified', 'time_roadway',
                'time_scene_cleared'
            ],
            'narrative': ['investigator_narrative_opinion'],
            'vehicle_driver_persons': [
                'address', 'autonomous_level_engaged', 'autonomous_unit', 'body_style',
                'cdl_end', 'dl_class', 'dl_id_num', 'dl_id_state', 'dl_id_type', 'dl_rest',
                'dob', 'fin_resp_name', 'fin_resp_number', 'fin_resp_phone_num',
                'fin_resp_type', 'hit_and_run', 'lp_num', 'lp_state', 'owner_address',
                'owner_lesse_tick_box', 'owner_name', 'parked_vehicle',
                'proof_of_fin_resp', 'towed_by', 'towed_to', 'unit_desc', 'unit_num',
                'veh_color', 'veh_make', 'veh_model', 'veh_year',
                'vehicle_damage_rating', 'vehicle_damage_rating2', 'vin', 
                {
                    'person_num': [
                        'person_description', 'person_name', 'person_num1',
                        'person_seat_position', 'person_type'
                    ]
                }
            ]
        }

    def _adjust_column_widths(self, writer, sheet_name, df):
        """Adjust column widths in Excel worksheet"""
        worksheet = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns):
            # Get maximum length of column items
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            ) + 2
            worksheet.set_column(idx, idx, min(max_length, 50))  # Cap width at 50

    def process_document_page_by_page(
        self, 
        input_file_path: str, 
        processor_id: str,
        cleanup: bool = True,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> Dict[str, Any]:
        """
        Process document pages concurrently with enhanced error handling
        
        Args:
            input_file_path (str): Path to the input PDF file
            processor_id (str): Document AI processor ID
            cleanup (bool): Whether to remove temporary page files after processing
            max_retries (int): Maximum number of retries for failed pages
            retry_delay (int): Delay between retries in seconds
        
        Returns:
            Dict[str, Any]: Processed document results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import concurrent.futures
        import time
        import logging
        
        # Configure logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(__name__)
        
        # Split PDF into individual page files
        page_splitter = DocumentPageSplitter(input_file_path)
        page_files = page_splitter.split_pdf_pages()
        
        # Initialize document result structure
        full_document_result = {
            "text": "",
            "pages": []
        }
        
        # Create progress tracking
        progress_bar = st.progress(0)
        total_pages = len(page_files)
        progress_text = st.empty()
        
        def process_page_with_retry(file_path: str, page_num: int) -> Dict[str, Any]:
            """
            Process a single page with retry mechanism
            
            Args:
                file_path (str): Path to the page file
                page_num (int): Page number
            
            Returns:
                Dict containing processed page data
            """
            for attempt in range(max_retries):
                try:
                    logger.info(f"Processing page {page_num}, attempt {attempt + 1}")
                    
                    # Process the page
                    processed_page = self.process_page(
                        processor_id=processor_id, 
                        file_path=file_path, 
                        page_number=page_num
                    )
                    
                    return processed_page
                
                except Exception as e:
                    logger.warning(f"Error processing page {page_num}, attempt {attempt + 1}: {str(e)}")
                    
                    # If this was the last retry, re-raise the exception
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to process page {page_num} after {max_retries} attempts")
                        raise
                    
                    # Wait before retrying
                    time.sleep(retry_delay)
        
        try:
            # Determine optimal number of workers
            max_workers = min(10, total_pages)
            
            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit processing tasks for each page
                future_to_page = {
                    executor.submit(
                        process_page_with_retry, 
                        file_path=page_file, 
                        page_num=i
                    ): i 
                    for i, page_file in enumerate(page_files, 1)
                }
                
                # Process completed futures in order
                processed_pages = [None] * total_pages
                
                # Collect results as they complete
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        # Update progress
                        progress_text.text(f"Processing page {page_num} of {total_pages}")
                        progress_bar.progress(page_num / total_pages)
                        
                        # Process the page
                        processed_page = future.result()
                        
                        # Prepare page data
                        page_data = {
                            "page_number": page_num,
                            "text": processed_page.get('text', ''),
                            "hierarchical_fields": {}
                        }
                        
                        # Process each section
                        for section, entities in processed_page.get('sections', {}).items():
                            if not entities:
                                continue
                            
                            # Initialize section in hierarchical fields if not exists
                            if section not in page_data["hierarchical_fields"]:
                                page_data["hierarchical_fields"][section] = []
                            
                            # Process each entity in the section
                            for entity in entities:
                                parent_entry = {
                                    "type": entity["type"],
                                    "value": entity.get('value', ''),
                                    "confidence": entity.get('confidence', 0),
                                    "child_fields": {}
                                }
                                
                                # Process child entities
                                for child in entity.get("child_entities", []):
                                    child_type = child["type"]
                                    if child_type not in parent_entry["child_fields"]:
                                        parent_entry["child_fields"][child_type] = []
                                    
                                    child_entry = {
                                        "type": child_type,
                                        "value": child.get('value', ''),
                                        "confidence": child.get('confidence', 0),
                                        "entities": []
                                    }
                                    
                                    # Process grandchild entities
                                    for grandchild in child.get("child_entities", []):
                                        entity_entry = {
                                            "type": grandchild["type"],
                                            "value": grandchild.get('value', ''),
                                            "confidence": grandchild.get('confidence', 0)
                                        }
                                        child_entry["entities"].append(entity_entry)
                                    
                                    parent_entry["child_fields"][child_type].append(child_entry)
                                
                                # Add parent entry to appropriate section
                                page_data["hierarchical_fields"][section].append(parent_entry)
                            
                        # Store page in the correct order
                        processed_pages[page_num - 1] = page_data
                    
                    except Exception as e:
                        logger.error(f"Error processing page {page_num}: {str(e)}")
                        # Store None for failed pages to maintain order
                        processed_pages[page_num - 1] = None
                
                # Filter out failed pages and add to final result
                successful_pages = [page for page in processed_pages if page is not None]
                full_document_result["pages"] = successful_pages
                
                # Log processing summary
                logger.info(f"Processed {len(successful_pages)} out of {total_pages} pages")
                
                # Combine text from all pages
                full_document_result["text"] = '\n'.join(
                    page.get('text', '') for page in full_document_result["pages"]
                )
        
        except concurrent.futures.CancelledError:
            logger.error("Document processing was cancelled")
            st.error("Document processing was cancelled")
            raise
        
        except Exception as e:
            logger.error(f"Error in concurrent document processing: {str(e)}")
            st.error(f"Error in document processing: {str(e)}")
            raise
        
        finally:
            # Clean up progress indicators
            progress_bar.empty()
            progress_text.empty()
            
            # Remove temporary page files if cleanup is requested
            if cleanup:
                for page_file in page_files:
                    try:
                        os.remove(page_file)
                    except Exception as cleanup_error:
                        logger.warning(f"Could not remove temporary file {page_file}: {cleanup_error}")
        
        return full_document_result

    def display_document_results(self, document_result: Dict[str, Any]):
        """Display document results with a flat hierarchy structure"""
        # Remove the duplicate "Document Analysis Results" header
        
        for page in document_result.get("pages", []):
            page_num = page["page_number"]
            with st.expander(f"Page {page_num}"):
                # Create sections for different parent types
                for parent_type, parent_entities in page.get("hierarchical_fields", {}).items():
                    if parent_entities:  # Only show sections with data
                        st.subheader(f"Section: {parent_type.replace('_', ' ').title()}")
                        
                        # Create tabs for each parent entity
                        if len(parent_entities) > 1:
                            parent_tabs = st.tabs([f"Entity {i+1}" for i in range(len(parent_entities))])
                        else:
                            parent_tabs = [st.container()]
                        
                        for parent_idx, (parent_entity, tab) in enumerate(zip(parent_entities, parent_tabs)):
                            with tab:
                                # Show parent entity information
                                if parent_entity.get('value'):
                                    st.markdown(f"**Value:** {parent_entity['value']}")
                                st.markdown(f"**Confidence:** {parent_entity['confidence']:.2%}")
                                
                                # Process child fields
                                child_fields = parent_entity.get("child_fields", {})
                                if child_fields:
                                    for child_type, child_entries in child_fields.items():
                                        if child_entries:
                                            st.markdown(f"### {child_type.replace('_', ' ').title()}")
                                            
                                            # Create DataFrame for child entries and their entities
                                            all_data = []
                                            
                                            for child_entry in child_entries:
                                                # Add child entry data
                                                if child_entry.get('value'):
                                                    child_row = {
                                                        "Level": "Child Field",
                                                        "Type": child_type,
                                                        "Value": child_entry['value'],
                                                        "Confidence": f"{child_entry['confidence']:.2%}"
                                                    }
                                                    all_data.append(child_row)
                                                
                                                # Add entity data
                                                for entity in child_entry.get("entities", []):
                                                    entity_row = {
                                                        "Level": "Entity",
                                                        "Type": entity['type'],
                                                        "Value": entity['value'],
                                                        "Confidence": f"{entity['confidence']:.2%}"
                                                    }
                                                    all_data.append(entity_row)
                                            
                                            if all_data:
                                                df = pd.DataFrame(all_data)
                                                st.dataframe(
                                                    df,
                                                    use_container_width=True,
                                                    hide_index=True
                                                )
                                
                                st.markdown("---")
                    
                    # Add a message if no entities found
                    if not page.get("hierarchical_fields"):
                        st.info("No entities found on this page")

    def save_to_gcs_as_json(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """
        Save the document processing results as JSON to Google Cloud Storage
        
        Args:
            bucket_name (str): Name of the GCS bucket
            data (Dict[str, Any]): Document processing results
            filename (str): Name of the file to save
            prefix (str, optional): Folder prefix in the bucket
            
        Returns:
            str: GCS URI of the saved JSON file
        """
        try:
            # Remove 'gs://' if present
            bucket_name = bucket_name.replace('gs://', '')
            
            # Create storage client
            bucket = self.storage_client.bucket(bucket_name)
            
            # Prepare the full blob path
            json_filename = filename.replace('.xlsx', '.json')
            full_blob_path = f"{prefix}/{json_filename}" if prefix else json_filename
            full_blob_path = full_blob_path.replace('//', '/')
            
            # Create JSON blob
            blob = bucket.blob(full_blob_path)
            
            # Convert data to JSON string with proper formatting
            json_str = json.dumps(data, indent=2, ensure_ascii=False)
            
            # Upload JSON to GCS
            blob.upload_from_string(
                json_str,
                content_type='application/json'
            )
            
            return f"gs://{bucket_name}/{full_blob_path}"
            
        except Exception as e:
            st.error(f"Error saving JSON to GCS: {str(e)}")
            raise

class CrashReportDataDictionary:
    """Data dictionary for Texas Peace Officer's Crash Report codes and values"""
    
    ROADWAY_SYSTEM = {
        'IH': 'Interstate',
        'US': 'US Highway',
        'SH': 'State Highway',
        'FM': 'Farm to Market',
        'RR': 'Ranch Road',
        'RM': 'Ranch to Market',
        'BI': 'Business Interstate',
        'BU': 'Business US',
        'BS': 'Business State',
        'BF': 'Business FM',
        'SL': 'State Loop',
        'TL': 'Toll Road',
        'AL': 'Alternate',
        'SP': 'Spur',
        'CR': 'County Road',
        'PR': 'Park Road',
        'PV': 'Private Road',
        'RC': 'Recreational Road',
        'LR': 'Local Road/Street'
    }
    
    ROADWAY_PART = {
        '1': 'Main/Proper Lane',
        '2': 'Service/Frontage Road',
        '3': 'Entrance/On Ramp',
        '4': 'Exit/Off Ramp',
        '5': 'Connector/Flyover',
        '98': 'Other'
    }
    
    DIRECTION = {
        'N': 'North',
        'E': 'East',
        'S': 'South',
        'W': 'West',
        'NE': 'Northeast',
        'SE': 'Southeast',
        'SW': 'Southwest',
        'NW': 'Northwest'
    }
    
    STREET_SUFFIX = {
        'RD': 'Road',
        'ST': 'Street',
        'DR': 'Drive',
        'LOOP': 'Loop',
        'EXPY': 'Expressway',
        'CT': 'Court',
        'CIR': 'Circle',
        'PL': 'Place',
        'PARK': 'Park',
        'CV': 'Cove',
        'PATH': 'Path',
        'TRC': 'Trace',
        'PT': 'Point',
        'AVE': 'Avenue',
        'BLVD': 'Boulevard',
        'PKWY': 'Parkway',
        'LN': 'Lane',
        'FWY': 'Freeway',
        'HWY': 'Highway',
        'WAY': 'Way',
        'TRL': 'Trail'
    }
    
    UNIT_DESCRIPTION = {
        '1': 'Motor Vehicle',
        '2': 'Train',
        '3': 'Pedalcyclist',
        '4': 'Pedestrian',
        '5': 'Motorized Conveyance',
        '6': 'Towed/Pushed/Trailer',
        '7': 'Non-Contact',
        '98': 'Other'
    }
    
    VEHICLE_COLOR = {
        'BGE': 'Beige',
        'BLK': 'Black',
        'BLU': 'Blue',
        'BRZ': 'Bronze',
        'BRO': 'Brown',
        'CAM': 'Camouflage',
        'CPR': 'Copper',
        'GLD': 'Gold',
        'GRY': 'Gray',
        'GRN': 'Green',
        'MAR': 'Maroon',
        'MUL': 'Multicolored',
        'ONG': 'Orange',
        'PNK': 'Pink',
        'PLE': 'Purple',
        'RED': 'Red',
        'SIL': 'Silver',
        'TAN': 'Tan',
        'TEA': 'Teal',
        'TRQ': 'Turquoise',
        'WHI': 'White',
        'YEL': 'Yellow',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    BODY_STYLE = {
        'P2': 'Passenger Car, 2-Door',
        'P4': 'Passenger Car, 4-Door',
        'PK': 'Pickup',
        'AM': 'Ambulance',
        'BU': 'Bus',
        'SB': 'Yellow School Bus',
        'FE': 'Farm Equipment',
        'FT': 'Fire Truck',
        'MC': 'Motorcycle',
        'PC': 'Police Car/Truck',
        'PM': 'Police Motorcycle',
        'TL': 'Trailer',
        'TR': 'Truck',
        'TT': 'Truck Tractor',
        'VN': 'Van',
        'EV': 'Neighborhood Vehicle',
        'SV': 'Sport Utility Vehicle',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    AUTONOMOUS_UNIT = {
        '1': 'Yes',
        '2': 'No',
        '99': 'Unknown'
    }
    
    AUTONOMOUS_LEVEL = {
        '0': 'No Automation',
        '1': 'Driver Assistance',
        '2': 'Partial Automation',
        '3': 'Conditional Automation',
        '4': 'High Automation',
        '5': 'Full Automation',
        '6': 'Automation Level Unknown',
        '99': 'Unknown'
    }
    
    PERSON_TYPE = {
        '1': 'Driver',
        '2': 'Passenger/Occupant',
        '3': 'Pedalcyclist',
        '4': 'Pedestrian',
        '5': 'Driver of Motorcycle Type Vehicle',
        '6': 'Passenger/Occupant on Motorcycle Type Vehicle',
        '95': 'Autonomous',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    INJURY_SEVERITY = {
        'A': 'Suspected Serious Injury',
        'B': 'Suspected Minor Injury',
        'C': 'Possible Injury',
        'K': 'Fatal Injury',
        'N': 'Not Injured',
        '95': 'Autonomous',
        '99': 'Unknown'
    }
    
    ETHNICITY = {
        'W': 'White',
        'B': 'Black',
        'H': 'Hispanic',
        'A': 'Asian',
        'I': 'American Indian/Alaskan Native',
        '95': 'Autonomous',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    FACTORS_AND_CONDITIONS = {
        '1': 'Animal on Road - Domestic',
        '2': 'Animal on Road - Wild',
        '3': 'Backed without Safety',
        '4': 'Changed Lane when Unsafe',
        '14': 'Disabled in Traffic Lane',
        '15': 'Disregard Stop and Go Signal',
        '16': 'Disregard Stop Sign or Light',
        '19': 'Distraction in Vehicle',
        '20': 'Driver Inattention',
        '22': 'Failed to Control Speed',
        '23': 'Failed to Drive in Single Lane',
        '40': 'Fatigued or Asleep',
        '41': 'Faulty Evasive Action',
        '42': 'Fire in Vehicle',
        '43': 'Fleeing or Evading Police',
        '44': 'Followed Too Closely',
        '45': 'Had Been Drinking',
        '67': 'Intoxicated - Alcohol',
        '68': 'Intoxicated - Drug',
        '73': 'Road Rage',
        '74': 'Cell/Mobile Device Use - Talking',
        '75': 'Cell/Mobile Device Use - Texting',
        '76': 'Cell/Mobile Device Use - Other',
        '77': 'Cell/Mobile Device Use - Unknown'
    }
    
    WEATHER_CONDITION = {
        '1': 'Clear',
        '2': 'Cloudy',
        '3': 'Rain',
        '4': 'Sleet/Hail',
        '5': 'Snow',
        '6': 'Fog',
        '7': 'Blowing Sand/Snow',
        '8': 'Severe Crosswinds',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    LIGHT_CONDITION = {
        '1': 'Daylight',
        '2': 'Dark, Not Lighted',
        '3': 'Dark, Lighted',
        '4': 'Dark, Unknown Lighting',
        '5': 'Dawn',
        '6': 'Dusk',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    ROADWAY_TYPE = {
        '1': 'Two-Way, Not Divided',
        '2': 'Two-Way, Divided, Unprotected Median',
        '3': 'Two-Way, Divided, Protected Median',
        '4': 'One-Way',
        '98': 'Other'
    }
    
    SURFACE_CONDITION = {
        '1': 'Dry',
        '2': 'Wet',
        '3': 'Standing Water',
        '4': 'Snow',
        '5': 'Slush',
        '6': 'Ice',
        '7': 'Sand, Mud, Dirt',
        '98': 'Other',
        '99': 'Unknown'
    }
    
    TRAFFIC_CONTROL = {
        '2': 'Inoperative',
        '3': 'Officer',
        '4': 'Flagman',
        '5': 'Signal Light',
        '6': 'Flashing Red Light',
        '7': 'Flashing Yellow Light',
        '8': 'Stop Sign',
        '9': 'Yield Sign',
        '10': 'Warning Sign',
        '11': 'Center Stripe/Divider',
        '12': 'No Passing Zone',
        '13': 'RR Gate/Signal',
        '15': 'Crosswalk',
        '16': 'Bike Lane',
        '17': 'Marked Lanes',
        '18': 'Signal Light With Red Light Running Camera',
        '96': 'None',
        '98': 'Other'
    }

    # New code mappings for more detailed person description processing
    SEX_CODES = {
        '1': 'Male',
        '2': 'Female',
        '99': 'Unknown'
    }
    
    EJECT_CODES = {
        '1': 'Not Ejected',
        '2': 'Partially Ejected',
        '3': 'Totally Ejected',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }
    
    RESTRAINT_CODES = {
        '1': 'Lap and Shoulder Belt',
        '2': 'Lap Belt Only',
        '3': 'Shoulder Belt Only',
        '4': 'Child Restraint - Forward Facing',
        '5': 'Child Restraint - Rear Facing',
        '6': 'Booster Seat',
        '7': 'None Used',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }
    
    AIRBAG_CODES = {
        '1': 'Deployed',
        '2': 'Not Deployed',
        '3': 'Deployed, Unknown Effectiveness',
        '4': 'Not Equipped',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }
    
    HELMET_CODES = {
        '1': 'Helmet Used',
        '2': 'No Helmet',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }
    
    SOBRIETY_CODES = {
        'Y': 'Yes',
        'N': 'No',
        '99': 'Unknown'
    }
    
    SUBSTANCE_SPEC_CODES = {
        '96': 'No Test Performed',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }
    
    SUBSTANCE_RESULT_CODES = {
        '1': 'Positive',
        '2': 'Negative',
        '96': 'No Test Performed',
        '97': 'Not Applicable',
        '99': 'Unknown'
    }

    @classmethod
    def get_description(cls, category: str, code: str) -> str:
        """
        Get the description for a code in a specific category
        
        Args:
            category: The category to look up (e.g., 'ROADWAY_SYSTEM')
            code: The code to look up
            
        Returns:
            The description for the code, or the original code if not found
        """
        try:
            category_dict = getattr(cls, category.upper())
            return category_dict.get(str(code), code)
        except AttributeError:
            return code

    @classmethod
    def decode_multiple(cls, category: str, codes: str) -> List[str]:
        """
        Decode multiple codes from a string
        
        Args:
            category: The category to look up
            codes: String of codes separated by spaces or commas
            
        Returns:
            List of descriptions for the codes
        """
        if not codes:
            return []
            
        # Split codes by space or comma
        code_list = [c.strip() for c in str(codes).replace(',', ' ').split()]
        return [cls.get_description(category, code) for code in code_list]

    @classmethod
    def is_valid_code(cls, category: str, code: str) -> bool:
        """
        Check if a code is valid for a category
        
        Args:
            category: The category to check
            code: The code to validate
            
        Returns:
            True if the code is valid, False otherwise
        """
        try:
            category_dict = getattr(cls, category.upper())
            return str(code) in category_dict
        except AttributeError:
            return False

class CrashReportDataProcessor:
    """Utility class for processing and transforming crash report data"""
    
    def __init__(self):
        self.processed_location = False
        self.data_dict = CrashReportDataDictionary()

    def process_location_data(self, location_data: Dict) -> Dict:
        """
        Process location data with decoded values to support Google Maps lookup
        
        Args:
            location_data: Raw location data dictionary
            
        Returns:
            Processed location data with structured address and decoded values
        """
        if self.processed_location:
            return {}
            
        address_components = {}
        decoded_components = {}
        
        for field_type, entries in location_data.get("child_fields", {}).items():
            if not entries:
                continue
                
            value = entries[0].get('value', '').strip()
            
            # Decode values based on field type
            if field_type == 'rdwy_sys':
                decoded_value = self.data_dict.get_description('ROADWAY_SYSTEM', value)
                decoded_components['roadway_system'] = decoded_value
            elif field_type == 'rdwy_part':
                decoded_value = self.data_dict.get_description('ROADWAY_PART', value)
                decoded_components['roadway_part'] = decoded_value
            elif field_type == 'dir_of_traffic':
                decoded_value = self.data_dict.get_description('DIRECTION', value)
                decoded_components['direction'] = decoded_value
            elif field_type == 'street_suffix':
                decoded_value = self.data_dict.get_description('STREET_SUFFIX', value)
                decoded_components['street_suffix'] = decoded_value
                
            # Store original values for address building
            if field_type in ['block_num', 'street_prefix', 'street_name', 'street_suffix']:
                address_components[field_type] = value
        
        # Build structured address
        address = " ".join(filter(None, [
            address_components.get('block_num', ''),
            address_components.get('street_prefix', ''),
            address_components.get('street_name', ''),
            address_components.get('street_suffix', '')
        ]))
        
        result = {
            'structured_address': address,
            'components': address_components,
            'decoded_values': decoded_components
        }
        
        self.processed_location = True
        return result

    def extract_person_description(self, description: str) -> Dict[str, str]:
        """
        Extract and decode detailed person description
        
        Args:
            description: Raw person description string
            
        Returns:
            Dictionary of decoded person information with structured details
        """
        # Initialize structured fields
        fields = {
            'injury_severity': '',
            'age': '',
            'ethnicity': '',
            'sex': '',
            'eject': '',
            'restr': '',
            'airbag': '',
            'helmet': '',
            'sol': '',
            'alc_spec': '',
            'alc_result': '',
            'drug_spec': '',
            'drug_result': '',
            'drug_category': ''
        }
        
        if not description:
            return fields
        
        # Clean and split description by lines
        parts = [p.strip() for p in description.split('\n') if p.strip()]
        
        try:
            # Ensure we have enough parts
            if len(parts) < 8:
                return fields
            
            # Parse fields in order
            fields['injury_severity'] = self.data_dict.get_description('INJURY_SEVERITY', parts[0])
            
            # Age (find first numeric value)
            age_parts = [p for p in parts if p.isdigit()]
            if age_parts:
                fields['age'] = age_parts[0]
            
            # Ethnicity 
            ethnicity_parts = [p for p in parts if p in self.data_dict.ETHNICITY]
            if ethnicity_parts:
                fields['ethnicity'] = self.data_dict.get_description('ETHNICITY', ethnicity_parts[0])
            
            # Sex
            sex_parts = [p for p in parts if p in self.data_dict.SEX_CODES]
            if sex_parts:
                fields['sex'] = self.data_dict.SEX_CODES.get(sex_parts[0], sex_parts[0])
            
            # Find parts for other fields
            remaining_parts = [p for p in parts if p not in [
                fields['injury_severity'], 
                fields.get('age', ''), 
                fields.get('ethnicity', ''), 
                fields.get('sex', '')
            ]]
            
            # Ensure at least 5 more parts are available
            if len(remaining_parts) >= 5:
                # Eject
                fields['eject'] = self.data_dict.EJECT_CODES.get(remaining_parts[0], remaining_parts[0])
                
                # Restraint
                fields['restr'] = self.data_dict.RESTRAINT_CODES.get(remaining_parts[1], remaining_parts[1])
                
                # Airbag
                fields['airbag'] = self.data_dict.AIRBAG_CODES.get(remaining_parts[2], remaining_parts[2])
                
                # Helmet
                fields['helmet'] = self.data_dict.HELMET_CODES.get(remaining_parts[3], remaining_parts[3])
            
            # Optional subsequent fields
            if len(remaining_parts) >= 6:
                # Sobriety of Last Drink
                fields['sol'] = self.data_dict.SOBRIETY_CODES.get(remaining_parts[4], remaining_parts[4])
            
            # Substance-related fields
            if len(remaining_parts) >= 8:
                # Alcohol specification and result
                fields['alc_spec'] = self.data_dict.SUBSTANCE_SPEC_CODES.get(remaining_parts[5], remaining_parts[5])
                fields['alc_result'] = self.data_dict.SUBSTANCE_RESULT_CODES.get(remaining_parts[6], remaining_parts[6])
                
                # Drug-related fields
                fields['drug_spec'] = self.data_dict.SUBSTANCE_SPEC_CODES.get(remaining_parts[7], remaining_parts[7])
                
                # Additional drug-related fields if available
                if len(remaining_parts) >= 10:
                    fields['drug_result'] = self.data_dict.SUBSTANCE_RESULT_CODES.get(remaining_parts[8], remaining_parts[8])
                    fields['drug_category'] = self.data_dict.SUBSTANCE_SPEC_CODES.get(remaining_parts[9], remaining_parts[9])
            
        except Exception as e:
            print(f"Error processing person description: {e}")
        
        return fields

    def process_vehicle_unit(self, vehicle_data: Dict) -> Dict:
        """
        Process vehicle unit data with decoded values
        
        Args:
            vehicle_data: Raw vehicle data dictionary
            
        Returns:
            Processed vehicle data with decoded values
        """
        processed_data = {
            'unit_info': {},
            'vehicle_info': {},
            'driver_info': None,
            'passengers': []
        }
        
        try:
            # Process unit description
            if 'unit_desc' in vehicle_data.get('child_fields', {}):
                value = vehicle_data['child_fields']['unit_desc'][0].get('value', '')
                processed_data['unit_info']['type'] = self.data_dict.get_description('UNIT_DESCRIPTION', value)
            
            # Process vehicle information
            for field in ['body_style', 'veh_color', 'veh_year', 'veh_make', 'veh_model']:
                if field in vehicle_data.get('child_fields', {}):
                    value = vehicle_data['child_fields'][field][0].get('value', '')
                    if field == 'body_style':
                        processed_data['vehicle_info'][field] = self.data_dict.get_description('BODY_STYLE', value)
                    elif field == 'veh_color':
                        processed_data['vehicle_info'][field] = self.data_dict.get_description('VEHICLE_COLOR', value)
                    else:
                        processed_data['vehicle_info'][field] = value
            
            # Process autonomous information
            if 'autonomous_unit' in vehicle_data.get('child_fields', {}):
                value = vehicle_data['child_fields']['autonomous_unit'][0].get('value', '')
                processed_data['vehicle_info']['autonomous'] = self.data_dict.get_description('AUTONOMOUS_UNIT', value)
            
            if 'autonomous_level_engaged' in vehicle_data.get('child_fields', {}):
                value = vehicle_data['child_fields']['autonomous_level_engaged'][0].get('value', '')
                processed_data['vehicle_info']['autonomous_level'] = self.data_dict.get_description('AUTONOMOUS_LEVEL', value)
            
            # Process person information
            person_data = self.process_person_data(vehicle_data)
            processed_data.update(person_data)
            
        except Exception as e:
            print(f"Error processing vehicle unit: {e}")
            
        return processed_data

    def process_person_data(self, vehicle_data: Dict) -> Dict:
        """
        Process person data with proper organization and decoded values
        
        Args:
            vehicle_data: Vehicle data dictionary containing person information
            
        Returns:
            Dictionary with processed driver and passenger information
        """
        processed_data = {'driver': None, 'passengers': []}
        passenger_count = 0
        
        try:
            for person in vehicle_data.get('child_fields', {}).get('person_num', []):
                person_info = {'details': {}}
                
                # Process basic information
                for entity in person.get('entities', []):
                    if entity['type'] == 'person_name':
                        person_info['name'] = entity['value']
                    elif entity['type'] == 'person_type':
                        person_info['type'] = self.data_dict.get_description('PERSON_TYPE', entity['value'])
                        # Store original code for sorting
                        person_info['type_code'] = entity['value']
                    elif entity['type'] == 'person_description':
                        person_info['details'] = self.extract_person_description(entity['value'])
                
                # Organize by driver/passenger
                if person_info.get('type_code') == '1':  # Driver
                    processed_data['driver'] = person_info
                else:
                    passenger_count += 1
                    person_info['passenger_num'] = passenger_count
                    processed_data['passengers'].append(person_info)
            
            # Sort passengers by seat position if available
            processed_data['passengers'].sort(key=lambda x: (
                x.get('details', {}).get('seat_position', ''),
                x.get('passenger_num', 0)
            ))
            
        except Exception as e:
            print(f"Error processing person data: {e}")
            
        return processed_data

    def convert_checkbox_value(self, value: str) -> Union[bool, str]:
        """
        Convert checkbox text to boolean or appropriate string value
        
        Args:
            value: Raw checkbox value
            
        Returns:
            Boolean True/False or string value
        """
        if not value:
            return False
            
        # Handle simple checkbox cases
        if '☑' in value:
            return True
        if '☐' in value:
            return False
            
        # Handle Yes/No text cases
        if 'Yes' in value:
            return True
        if 'No' in value:
            return False
            
        # Return original value if not a checkbox
        return value.strip()

    def clean_narrative(self, narrative: str) -> str:
        """
        Clean narrative text by removing tags and normalizing whitespace
        
        Args:
            narrative: Raw narrative text
            
        Returns:
            Cleaned narrative text
        """
        if not narrative:
            return ""
            
        # Remove <cr> tags
        cleaned = narrative.replace('<cr>', ' ')
        
        # Remove any other HTML-like tags
        cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
        
        # Normalize whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned

    def process_factors_and_conditions(self, factors_data: Dict) -> Dict:
        """
        Process factors and conditions with decoded values
        
        Args:
            factors_data: Raw factors and conditions data
            
        Returns:
            Processed factors data with decoded values
        """
        processed_data = {
            'contributing_factors': [],
            'vehicle_defects': [],
            'environmental_conditions': {}
        }
        
        try:
            # Process contributing factors
            if 'contributing_factors' in factors_data.get('child_fields', {}):
                for factor in factors_data['child_fields']['contributing_factors']:
                    value = factor.get('value', '')
                    decoded = self.data_dict.get_description('FACTORS_AND_CONDITIONS', value)
                    processed_data['contributing_factors'].append(decoded)
            
            # Process environmental conditions
            condition_mappings = {
                'weather_cond': 'WEATHER_CONDITION',
                'light_cond': 'LIGHT_CONDITION',
                'road_type': 'ROADWAY_TYPE',
                'surface_cond': 'SURFACE_CONDITION',
                'traffic_control': 'TRAFFIC_CONTROL'
            }
            
            for field, category in condition_mappings.items():
                if field in factors_data.get('child_fields', {}):
                    value = factors_data['child_fields'][field][0].get('value', '')
                    processed_data['environmental_conditions'][field] = self.data_dict.get_description(category, value)
            
        except Exception as e:
            print(f"Error processing factors and conditions: {e}")
            
        return processed_data

    def sort_vehicles_by_unit(self, data: List[Dict]) -> List[Dict]:
        """
        Sort vehicle data by unit number
        
        Args:
            data: List of vehicle data dictionaries
            
        Returns:
            Sorted list of vehicle data
        """
        def get_unit_num(vehicle):
            try:
                return int(vehicle.get('child_fields', {}).get('unit_num', [{}])[0].get('value', '0'))
            except (ValueError, IndexError):
                return 0
                
        return sorted(data, key=get_unit_num)

class CheckboxProcessor:
    """Process checkbox values from JSON extraction"""
    
    # Specific fields to process as checkboxes
    CHECKBOX_FIELDS = {
        'outside_city_limit',
        'crash_damage_1000',
        'const_zone',
        'worker_present',
        'parked_vehicle',
        'owner_lesse_tick_box',
        'hit_and_run',
        'proof_of_fin_resp',
        'investigation_complete',
        'owner_lesse'
    }
    
    @staticmethod
    def parse_checkbox_value(value: str) -> Union[bool, str]:
        """
        Parse checkbox value with advanced detection logic
        
        Args:
            value: Raw checkbox value from JSON
            
        Returns:
            bool or str: Processed value
        """
        if not value:
            return False
        
        # Clean and normalize the value
        value = value.replace('\n', ' ').strip()
        
        # Direct boolean check based on checkbox symbols
        if value.count('☑') == 1 and value.count('☐') == 0:
            return True
        if value.count('☐') == 1 and value.count('☑') == 0:
            return False
        
        # Process multiple option cases
        parts = value.split()
        selected_options = []
        is_checkbox = '☑' in value
        
        for part in parts:
            # Detect selected option
            if '☑' in part or (is_checkbox and part in ['Yes', 'True']):
                selected_options.append(part)
            # Handle Yes/No scenarios
            elif part in ['Yes', 'No']:
                if '☑' in value and part == 'Yes':
                    return True
                elif '☑' in value and part == 'No':
                    return False
        
        # Final processing of selected options
        if selected_options:
            # Remove checkbox symbols
            cleaned_options = [opt for opt in selected_options if opt not in ['☑', '☐']]
            
            if not cleaned_options:
                return True  # Checkbox is checked
            
            # Join and clean the selected options
            result = ' '.join(cleaned_options).strip()
            
            # Convert to boolean for Yes/No
            if result.lower() == 'yes':
                return True
            if result.lower() == 'no':
                return False
            
            return result
        
        return False

    @staticmethod
    def process_json_field(field_data: Dict) -> Dict:
        """
        Process a JSON field with advanced checkbox handling
        
        Args:
            field_data: Field data dictionary from JSON
            
        Returns:
            Processed field data
        """
        result = {
            'type': field_data.get('type', ''),
            'confidence': field_data.get('confidence', 0)
        }
        
        value = field_data.get('value', '')
        field_type = result['type'].lower()
        
        # Determine if field should be processed as a checkbox
        is_checkbox_field = (
            field_type in CheckboxProcessor.CHECKBOX_FIELDS or 
            'tick_box' in field_type or 
            any(cb_field in field_type for cb_field in CheckboxProcessor.CHECKBOX_FIELDS)
        )
        
        if is_checkbox_field:
            # Process as checkbox
            processed_value = CheckboxProcessor.parse_checkbox_value(value)
            
            result['value'] = processed_value
            
            # Raw value is only set if the processed value is not a simple boolean
            result['raw_value'] = (
                value if processed_value is not True and processed_value is not False 
                else ''
            )
        else:
            # Non-checkbox fields keep original value
            result['value'] = value
            result['raw_value'] = ''
        
        return result

    @staticmethod
    def process_nested_json(json_data: Dict) -> Dict:
        """
        Process nested JSON data with checkbox values
        
        Args:
            json_data: Nested JSON dictionary
            
        Returns:
            Processed JSON data
        """
        result = {}
        
        for key, value in json_data.items():
            if isinstance(value, dict):
                result[key] = CheckboxProcessor.process_nested_json(value)
            elif isinstance(value, list):
                result[key] = [
                    CheckboxProcessor.process_nested_json(item) if isinstance(item, dict)
                    else item
                    for item in value
                ]
            elif key == 'child_fields':
                result[key] = {
                    field_key: [CheckboxProcessor.process_json_field(field) for field in fields]
                    for field_key, fields in value.items()
                }
            else:
                result[key] = value
                
        return result

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
    
    # Initialize session state variables
    if 'document_result' not in st.session_state:
        st.session_state.document_result = None
    if 'excel_output_filename' not in st.session_state:
        st.session_state.excel_output_filename = None
    if 'output_bucket' not in st.session_state:
        st.session_state.output_bucket = PROJECT_CONFIG['output_bucket']
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    
    # File upload
    uploaded_file = st.file_uploader("Choose a PDF document", type=['pdf'])
    
    # Process button
    if st.button("Process Document"):
        if uploaded_file is not None:
            try:
                st.session_state.processing_complete = False
                
                # Initialize processor
                processor = DocumentAIProcessor(
                    project_id=PROJECT_CONFIG['project_id'],
                    location=PROJECT_CONFIG['location']
                )
                
                # Generate unique filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                original_filename = uploaded_file.name
                base_name, ext = os.path.splitext(original_filename)
                input_filename = f"{base_name}_{timestamp}{ext}"
                output_filename = f"{base_name}_{timestamp}"
                
                # Save uploaded file
                with open(input_filename, "wb") as f:
                    f.write(uploaded_file.getvalue())
                
                # Process document
                st.session_state.document_result = processor.process_document_page_by_page(
                    input_file_path=input_filename,
                    processor_id=PROJECT_CONFIG['processor_id']
                )
                
                # Save JSON to GCS first
                processor.save_to_gcs_as_json(
                    bucket_name=PROJECT_CONFIG['output_bucket'],
                    data=st.session_state.document_result,
                    filename=output_filename,
                    prefix="output"
                )
                
                # Then save Excel
                excel_gcs_uri = processor.save_excel_to_gcs(
                    bucket_name=PROJECT_CONFIG['output_bucket'],
                    data=st.session_state.document_result,
                    filename=f"{output_filename}.xlsx",
                    prefix="output"
                )
                
                st.session_state.excel_output_filename = f"output/{output_filename}.xlsx"
                st.session_state.excel_gcs_uri = excel_gcs_uri
                st.session_state.processing_complete = True
                
                st.success("Document processed successfully!")
                processor.display_document_results(st.session_state.document_result)
                
            except Exception as e:
                st.error(f"Error processing document: {str(e)}")
                st.session_state.processing_complete = False
            finally:
                # Clean up
                if os.path.exists(input_filename):
                    os.remove(input_filename)
        else:
            st.warning("Please upload a PDF document")
    
    # Only show results once
    if st.session_state.document_result is not None:
        processor = DocumentAIProcessor(
            project_id=PROJECT_CONFIG['project_id'],
            location=PROJECT_CONFIG['location']
        )
        # Remove the duplicate header
        st.markdown("## Document Analysis Results")
        processor.display_document_results(st.session_state.document_result)
    
    # Add Excel download button if processing is complete
    if st.session_state.processing_complete:
        try:
            excel_bytes = download_file_from_gcs(
                bucket_name=st.session_state.output_bucket,
                source_blob_name=st.session_state.excel_output_filename
            )
            
            st.download_button(
                label="📥 Download Excel Report",
                data=excel_bytes,
                file_name=os.path.basename(st.session_state.excel_output_filename),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Error preparing download: {str(e)}")

if __name__ == "__main__":
    main()