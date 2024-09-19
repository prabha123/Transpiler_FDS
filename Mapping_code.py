# Databricks notebook source
#TODO: Wrap if statement to the start of process_xml_file function
import lxml.etree
import pandas
import pathlib

# Function that processes xml data 
def process_xml_file(xml_file: str) -> None:
    with open(xml_file, 'r') as file:
        xml_data = file.read()

    root = lxml.etree.fromstring(xml_data)

    # Constructing filename for output parquet file
    filename_struct = 'Form_' + root.get('formName') + '_' + root.get('version') + '-' + root.tag
    
    # Check if the directory exists & generate output file name
    output_dir = '/Workspace/Users/ahmed.muse@accenture.com/BOE Mapping/Mappings/csv/'
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = output_dir + filename_struct

    # Generate empty list to append into dataframe 
    data = []
    # Looping through list of boxCodeMapping
    for mapping in root.findall('boxCodeMapping'):
        # Loop through attributes in an individual boxCodeMapping and append to empty dict
        entry = {}
        for key in mapping.attrib:
            entry[key] = mapping.attrib.get(key)
        data.append(entry)

    # Create Dataframe
    df = pandas.DataFrame(data)

    # Check if the file exists and create csv file from dataframe
    if not pathlib.Path(output_file).exists():
        df.to_csv(output_file)
        print(f'New csv file named {filename_struct} created in dir: {output_dir}')
    else:
        print(f'File named {filename_struct} already exists in dir: {output_dir}')
    

def loop_through_files(dir: str) -> None:
    pathlist = pathlib.Path(dir).glob('*.xml')

    for path in pathlist:
        process_xml_file(str(path))

# Call the function with the XML file path
directory = '/Workspace/Users/ahmed.muse@accenture.com/BOE Mapping/Mappings/'
loop_through_files(directory)
