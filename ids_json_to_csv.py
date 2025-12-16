import json
import csv
import pandas as pd
import sys

def convert_json_to_csv(input_file, output_file, key_path='data', column_name='STUDENT_ID'):
    try:
        # Load JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)
            
        # Safely extract the data list
        student_records = data.get(key_path, [])
        
        # Extract the ID from each record
        student_ids = [record.get(column_name) for record in student_records if record.get(column_name) is not None]
        
        # Use pandas to easily create and save the CSV
        df = pd.DataFrame(student_ids, columns=[column_name])
        
        # Write the data to a CSV file (index=False prevents adding row numbers)
        df.to_csv(output_file, index=False, header=True)
        
        print(f"Successfully created CSV file: {output_file} with {len(student_ids)} records.")
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.", file=sys.stderr)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{input_file}'.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    convert_json_to_csv("student_ids.json", "OSU-Events-Users.csv")