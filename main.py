import streamlit as st
import pandas as pd
import json
import os
import shutil
import PyPDF2
import io
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
    "input_bucket": "doc-ai-extraction-dev",
    "output_bucket": "doc-ai-extraction-dev"
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

    def match_string_for_boolean(self, types: str, string: str) -> str:
        elgible_type = ["outside_city_limit", "crash_damage_1000", 
                        'owner_lesse_tick_box','proof_of_fin_resp','investigation_complete']
        if types.lower() in elgible_type:
            return "true" if "☑" in string.lower() else "false"
        return string

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

    def save_excel_to_gcs(self, bucket_name: str, data: Dict[str, Any], filename: str, prefix: str = '') -> str:
        """Save hierarchical data to Excel with multiple sheets and organized location sections"""
        try:
            # Create Excel writer with xlsxwriter engine
            with pd.ExcelWriter('temp_output.xlsx', engine='xlsxwriter') as writer:
                # Process each page
                for page in data.get("pages", []):
                    page_num = page["page_number"]
                    
                    # Process identification_location sections
                    id_locations = page.get("hierarchical_fields", {}).get("identification_location", [])
                    if id_locations:
                        sheet_name = f"P{page_num}_identification_location"[:31]
                        rows = []
                        
                        # Process each identification_location section
                        section_names = ["General Information", "Road of Crash", "Intersecting Road"]
                        
                        for idx, location in enumerate(id_locations):
                            # Add section header
                            if idx < len(section_names):
                                section_header = {
                                    "Page": page_num,
                                    "Level": "Section Header",
                                    "Type": section_names[idx],
                                    "Value": "",
                                    "Confidence": ""
                                }
                                rows.append(section_header)
                            
                            # Process fields within each section
                            # Initialize an empty dictionary to store street address components
                            street_address = {}

                            # Define eligible field types for street address components
                            eligible_types = ["block_num", "street_name", "street_prefix", "street_suffix"]

                            # Loop through child fields in the location dictionary
                            for field_type, field_entries in location.get("child_fields", {}).items():
                                # Convert field_type to lowercase for case-insensitive comparison
                                field_type_lower = field_type.lower()

                                # Loop through each entry in the field_entries list
                                for entry in field_entries:
                                    # If the field type is not eligible, add it to the rows list
                                    if field_type_lower not in eligible_types:
                                        field_row = {
                                            "Page": page_num,
                                            "Level": "Field",
                                            "Type": field_type,
                                            "Value": self.match_string_for_boolean(field_type, entry.get("value", "")),
                                            "Confidence": f"{entry.get('confidence', 0):.2%}"
                                        }
                                        rows.append(field_row)
                                    else:
                                        # If the field type is 'street_suffix', construct the full street address
                                        if field_type_lower == "street_suffix":
                                            # Construct the full street address using components from street_address
                                            full_address = (
                                                f'{street_address.get("block_num", "")} '
                                                f'{street_address.get("street_prefix", "")} '
                                                f'{street_address.get("street_name", "")} '
                                                f'{entry.get("value", "")}'
                                            ).strip()  # Remove any extra spaces

                                            # Add the full street address to the rows list
                                            field_row = {
                                                "Page": page_num,
                                                "Level": "Field",
                                                "Type": "street_address",
                                                "Value": full_address,
                                                "Confidence": f"{entry.get('confidence', 0):.2%}"
                                            }
                                            rows.append(field_row)
                                        else:
                                            # Store the value in the street_address dictionary for later use
                                            street_address[field_type_lower] = entry.get("value", "")
                                    
                            
                            # Add separator after each section
                            separator_row = {
                                "Page": page_num,
                                "Level": "Separator",
                                "Type": "",
                                "Value": "",
                                "Confidence": ""
                            }
                            rows.append(separator_row)
                        
                        if rows:
                            df = pd.DataFrame(rows)
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            
                            # Format the worksheet
                            worksheet = writer.sheets[sheet_name]
                            workbook = writer.book
                            
                            # Create formats
                            header_format = workbook.add_format({
                                'bold': True,
                                'bg_color': '#D3D3D3',
                                'align': 'center'
                            })
                            
                            separator_format = workbook.add_format({
                                'bottom': 1
                            })
                            
                            # Apply formats
                            for row_idx, row in enumerate(rows, 1):
                                if row.get('Level') == 'Section Header':
                                    worksheet.set_row(row_idx, None, header_format)
                                elif row.get('Level') == 'Separator':
                                    worksheet.set_row(row_idx, None, separator_format)
                            
                            # Adjust column widths
                            self._adjust_column_widths(writer, sheet_name, df)
                    
                    # Process other sections (vehicle_driver_persons, etc.)
                    for parent_type, parent_entities in page.get("hierarchical_fields", {}).items():
                        # Skip identification_location as it's already processed
                        if parent_type == 'identification_location':
                            continue
                            
                        # Tracking unique identifiers for sections
                        section_unique_trackers = {}
                        
                        if parent_type == 'vehicle_driver_persons':
                            # Create a separate sheet for each parent entity
                            for parent_idx, parent_entity in enumerate(parent_entities, 1):
                                sheet_name = f"P{page_num}_vehicle_driver_{parent_idx}"[:31]
                                rows = []
                                
                                # Add parent information
                                parent_row = {
                                    "Page": page_num,
                                    "Level": "Parent",
                                    "Type": parent_type,
                                    "Value": parent_entity.get('value', ''),
                                    "Confidence": f"{parent_entity.get('confidence', 0):.2%}"
                                }
                                rows.append(parent_row)
                                
                                # Process child fields
                                for child_type, child_entries in parent_entity.get("child_fields", {}).items():
                                    if child_type == 'person_num':
                                        for person_idx, child_entry in enumerate(child_entries, 1):
                                            # Add person header
                                            person_header_row = {
                                                "Page": page_num,
                                                "Level": "Person Header",
                                                "Type": f"Person {person_idx}",
                                                "Value": f"Person {person_idx} Details",
                                                "Confidence": ""
                                            }
                                            rows.append(person_header_row)
                                            
                                            # Process person entities
                                            for entity in child_entry.get("entities", []):
                                                entity_row = {
                                                    "Page": page_num,
                                                    "Level": "Entity",
                                                    "Type": str(entity.get('type', '')).replace('_', f'{person_idx}_'),
                                                    "Value": entity.get('value', ''),
                                                    "Confidence": f"{entity.get('confidence', 0):.2%}"
                                                }
                                                rows.append(entity_row)
                                            
                                            # Add separator
                                            rows.append({
                                                "Page": page_num,
                                                "Level": "Separator",
                                                "Type": "",
                                                "Value": "",
                                                "Confidence": ""
                                            })
                                    else:
                                        # Process other child fields
                                        for child_entry in child_entries:
                                            child_row = {
                                                "Page": page_num,
                                                "Level": "Child",
                                                "Type": child_type,
                                                "Value": self.match_string_for_boolean(child_type, child_entry.get('value', '')),
                                                "Confidence": f"{child_entry.get('confidence', 0):.2%}"
                                            }
                                            rows.append(child_row)
                                
                                if rows:
                                    df = pd.DataFrame(rows)
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                                    
                                    # Format worksheet
                                    worksheet = writer.sheets[sheet_name]
                                    workbook = writer.book
                                    header_format = workbook.add_format({
                                        'bold': True,
                                        'bg_color': '#D3D3D3',
                                        'align': 'center'
                                    })
                                    
                                    separator_format = workbook.add_format({
                                        'bottom': 1
                                    })
                                    
                                    for row_idx, row in enumerate(rows, 1):
                                        if row.get('Level') == 'Person Header':
                                            worksheet.set_row(row_idx, None, header_format)
                                        elif row.get('Level') == 'Separator':
                                            worksheet.set_row(row_idx, None, separator_format)
                                    
                                    self._adjust_column_widths(writer, sheet_name, df)
                        else:
                            # Handle other section types
                            if parent_type not in section_unique_trackers:
                                section_unique_trackers[parent_type] = 0
                            section_unique_trackers[parent_type] += 1
                            
                            sheet_name = f"P{page_num}_{parent_type}_{section_unique_trackers[parent_type]}"[:31]
                            rows = []
                            
                            for parent_entity in parent_entities:
                                parent_row = {
                                    "Page": page_num,
                                    "Level": "Parent",
                                    "Type": parent_type,
                                    "Value": parent_entity.get('value', ''),
                                    "Confidence": f"{parent_entity.get('confidence', 0):.2%}"
                                }
                                rows.append(parent_row)
                                
                                for child_type, child_entries in parent_entity.get("child_fields", {}).items():
                                    for child_entry in child_entries:
                                        child_row = {
                                            "Page": page_num,
                                            "Level": "Child",
                                            "Type": child_type,
                                            "Value": self.match_string_for_boolean(child_type, child_entry.get('value', '')),
                                            "Confidence": f"{child_entry.get('confidence', 0):.2%}"
                                        }
                                        rows.append(child_row)
                                        
                                        for entity in child_entry.get("entities", []):
                                            entity_row = {
                                                "Page": page_num,
                                                "Level": "Entity",
                                                "Type": str(entity.get('type', '')).replace('_', f'{person_idx}_'),
                                                "Value": entity.get('value', ''),
                                                "Confidence": f"{entity.get('confidence', 0):.2%}"
                                            }
                                            rows.append(entity_row)
                            
                            if rows:
                                df = pd.DataFrame(rows)
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                                self._adjust_column_widths(writer, sheet_name, df)
            
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
        cleanup: bool = True
    ) -> Dict[str, Any]:
        """
        Process document pages concurrently using ThreadPoolExecutor
        
        Args:
            input_file_path (str): Path to the input PDF file
            processor_id (str): Document AI processor ID
            cleanup (bool): Whether to remove temporary page files after processing
        
        Returns:
            Dict[str, Any]: Processed document results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import concurrent.futures
        
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
        
        try:
            # Determine optimal number of workers
            max_workers = min(10, total_pages)
            
            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit processing tasks for each page
                future_to_page = {
                    executor.submit(
                        self.process_page, 
                        processor_id=processor_id, 
                        file_path=page_file, 
                        page_number=i
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
                        st.error(f"Error processing page {page_num}: {str(e)}")
                        # Store None for failed pages to maintain order
                        processed_pages[page_num - 1] = None
                
                # Filter out failed pages and add to final result
                full_document_result["pages"] = [
                    page for page in processed_pages if page is not None
                ]
                
                # Combine text from all pages
                full_document_result["text"] = '\n'.join(
                    page.get('text', '') for page in full_document_result["pages"]
                )
        
        except concurrent.futures.CancelledError:
            st.error("Document processing was cancelled")
            raise
        
        except Exception as e:
            st.error(f"Error in concurrent document processing: {str(e)}")
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
                        st.warning(f"Could not remove temporary file {page_file}: {cleanup_error}")
        
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