
import requests
import csv
import sys
import os
from dotenv import load_dotenv
import certifi
import urllib3
import datetime
from firebase_admin import credentials, initialize_app, firestore

load_dotenv()
client_id = os.getenv("OSU_API_KEY")
client_secret = os.getenv("OSU_API_SECRET")

# confiugres Firestore database connection
try:
    FIREBASE_CRED_PATH = os.getenv("FIREBASE_CRED_PATH")
    cred = credentials.Certificate(FIREBASE_CRED_PATH)
    initialize_app(cred)
    db = firestore.client()
    print("Firestore database connected!")
except Exception as e:
    # Firebase credentials or initialization fails
    print(f"Warning: Could not initialize Firebase Admin SDK. Usage data column will be skipped or may fail: {e}", file=sys.stderr)
    db = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Authenticates with the Oregon State API and returns an access token.
def get_access_token(client_id, client_secret):
    url = "https://developer.oregonstate.edu/api-proxy/osu-api?_api_proxy_uri=oauth2%2Ftoken"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = f"grant_type=client_credentials&scope=full&client_id={client_id}&client_secret={client_secret}"
    response = requests.post(url, headers=headers, data=data, verify=False)
    response.raise_for_status()
    return response.json()["access_token"]

# Fetches the department details for a student from the Oregon State API.
def get_student_department(student_id, access_token):
    url = f"https://api.oregonstate.edu/v1/students/{student_id}/degrees?term=current"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()

# Determines the department of a student based on their academic data.
def determine_department(student_data):
    is_engineering = False
    is_honors = False
    colleges = []
    for degree_data in student_data.get("data", []):
        attributes = degree_data.get("attributes", {})
        if attributes.get("college") == "College of Engineering":
            is_engineering = True
        if attributes.get("degree", "").startswith("Honors"):
            is_honors = True
        colleges.append(attributes.get("college"))
    
    # if a user is only in one college --> no secondary college --> colleges[1] = "N/A"
    if (len(colleges) == 1):
        colleges.append("N/A")

    if is_engineering and is_honors:
        return [colleges, "Both"]
    elif is_engineering:
        return [colleges, "COE"]
    elif is_honors:
        return [colleges, "HC"]
    return [colleges, "Unknown"]

# Fetches the college affiliation (College of Engineering, College of Business, etc.) of a student using the Oregon State API.
def get_student_college(student_id, access_token):
    url = f"https://api.oregonstate.edu/v1/students/{student_id}/degrees?term=current"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()

#Fetches the academic classification information of a student using the Oregon State API.
def get_student_year(student_id, access_token):
    url = f"https://api.oregonstate.edu/v1/students/{student_id}/classification"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()

# determines the grade level of a student (Freshman, Sophomore, Junior, Senior, Graudate)
def determine_student_year(student_data):
    degree_data = student_data.get("data", [])
    attributes = degree_data.get("attributes", {})
    classification = attributes.get("classification")
    if classification == "Determine from Student Type":
        return "Graduate"
    return classification

# creates a dictionary for each student ID showing the number of days they used the app over the last 30 days
def get_usage_data(db):
    if db is None:
        return {}
    
    usage_counts = {}
    today = datetime.date.today()

    # define date range here (currently set for last 90 days)
    date_strings = []
    for i in range(90):
        date_str = (today - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        date_strings.append(date_str)
    
    print("Dates:")
    print(date_strings)

    try:
        usage_collection = db.collection("StudentUsage")

        for date_str in date_strings:
            doc_ref = usage_collection.document(date_str)
            doc = doc_ref.get()

            if doc.exists:
                data = doc.to_dict()

                date_map = data.get(date_str, {})
                opened_ids = data.get("openedIDs", [])

                for student_id in opened_ids:
                    clean_id = str(student_id).strip('"')
                    usage_counts[clean_id] = usage_counts.get(clean_id, 0) + 1
            
            else:
                print(f"No usage data found for date: {date_str}", file=sys.stderr)
    
    except Exception as e:
        print(f"Error fetching usage data from Firestore: {e}", file=sys.stderr)
        return {}
    
    return usage_counts

#Processes student IDs from a CSV file and writes the output to a specified file.
def process_student_ids(input_file, output_file, access_token, usage_counts):
    with open(output_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["STUDENT_ID", "CLASSIFICATION", "PRIMARY_COLLEGE", "SECONDARY_COLLEGE", "HC_OR_COE", "USAGE_OVER_LAST_90_DAYS"])
        
        with open(input_file, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row: # skips empty rows
                    continue
                
                student_id = row[0].strip() # ensures student_id is clean
                if not student_id:
                    continue

                usage_count = usage_counts.get(student_id, 0)
                
                try:
                    student_department_data = get_student_department(student_id, access_token)
                    department = determine_department(student_department_data)

                    if department[1] != "Unknown":
                        student_year_data = get_student_year(student_id, access_token)
                        year = determine_student_year(student_year_data)
                        if (department[0][0] == department[0][1]): # special case: graduate students may have the same college listed for both primary & secondary degrees
                            department[0][1] = "N/A"
                        writer.writerow([student_id, year, department[0][0], department[0][1], department[1], usage_count])
                except requests.exceptions.RequestException as e:
                    print(f"Could not process student ID {student_id}: {e}", file=sys.stderr)

def main():
    """Main function to execute the script."""
    # client_id = os.environ.get("OSU_API_KEY")
    # client_secret = os.environ.get("OSU_API_SECRET")
    
    if not client_id or not client_secret:
        print("Error: OSU_API_KEY and OSU_API_SECRET environment variables must be set.", file=sys.stderr)
        sys.exit(1)
        
    try:
        access_token = get_access_token(client_id, client_secret)
        usage_data = get_usage_data(db)
        print("Processing student data. This might take a while...")
        process_student_ids("OSU-Events-Users.csv", "output.csv", access_token, usage_data)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
