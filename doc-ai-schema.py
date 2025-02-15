from google.cloud import documentai_v1

def create_crash_report_schema(
    project_id: str,
    location: str,
    processor_id: str,
    schema_display_name: str
):
    """
    Creates a Document AI schema for crash report with multiple parent-child entities
    """
    # Use DocumentAI client
    client = documentai_v1.DocumentProcessorServiceClient()
    
    # Get the location path for schema creation
    parent = f"projects/{project_id}/locations/{location}"
    processor_path = f"{parent}/processors/{processor_id}"
    
    # Create schema definition
    schema = documentai_v1.Schema()
    
    # Helper function to create a field
    def create_field(name, display_name, type_="STRING"):
        field = documentai_v1.Schema.EntityType.Property()
        field.name = name
        field.display_name = display_name
        field.type_ = type_
        return field
    
    # Create entity types with properties
    def create_identification_location_entity():
        entity = documentai_v1.Schema.EntityType()
        entity.name = "identification_location"
        entity.display_name = "Identification Location"
        
        # Add properties to the entity
        entity.properties.extend([
            create_field("case_id", "Case ID"),
            create_field("crash_date", "Crash Date", "DATE"),
            create_field("crash_time", "Crash Time"),
            create_field("latitude", "Latitude"),
            create_field("longitude", "Longitude"),
            create_field("street_name", "Street Name"),
            # Add more properties as needed
        ])
        
        return entity
    
    # Create multiple entity types
    schema.entity_types.extend([
        create_identification_location_entity(),
        # Add more entity types as needed
    ])
    
    # Request to update processor schema
    request = documentai_v1.UpdateProcessorRequest(
        processor=processor_path,
        processor_version_schema=schema
    )
    
    try:
        # Update the processor schema
        operation = client.update_processor(request=request)
        result = operation.result()
        print(f"Updated processor schema: {result}")
        return result
    except Exception as e:
        print(f"Error updating processor schema: {str(e)}")
        raise

def main():
    project_id = "neon-camp-449123-j1"
    location = "us"
    processor_id = "65b51dc1bf01ad16"
    schema_name = "crash_report_schema"
    
    try:
        schema = create_crash_report_schema(
            project_id=project_id,
            location=location,
            processor_id=processor_id,
            schema_display_name=schema_name
        )
        print(f"Successfully updated processor schema")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()