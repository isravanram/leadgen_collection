import streamlit as st
import requests
import re
import sys

# Custom CSS for styling
st.markdown(
    """
    <style>
    body {
        background: linear-gradient(to right, #1e3c72, #2a5298);
        font-family: 'Poppins', sans-serif;
        color: #ffffff;
    }
    .sidebar .sidebar-content {
        background: #ffffff;
        padding: 10px;
        border-radius: 10px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.2);
    }
    .main-title {
        text-align: center;
        color: #ffffff;
        font-size: 40px;
        font-weight: 700;
        margin-top: 20px;
    }
    .sub-title {
        color: #dcdde1;
        font-size: 22px;
        margin-bottom: 10px;
    }
    .filters {
        background: rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 15px;
        box-shadow: 0 0 15px rgba(0, 0, 0, 0.3);
        margin-bottom: 20px;
    }
    .stTextInput > div > div > input {
        background: #ffffff;
        color: #333;
        border: none;
        border-radius: 5px;
        padding: 10px;
    }
    .footer {
        text-align: center;
        margin-top: 50px;
        color: #bdc3c7;
        font-size: 14px;
    }
    .stButton > button {
        background: #6c63ff;
        color: #ffffff;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        font-size: 16px;
        font-weight: 600;
        cursor: pointer;
    }
    .stButton > button:hover {
        background: #5146d9;
    }
    </style>
    """,
    unsafe_allow_html=True
)

def execute_error_block(error_message):
    print('============== ERROR BLOCK ==============')
    print(error_message)
    print(f"\n------------Stopping the program ------------")
    sys.exit()

# Function to validate the input format
def validate_employee_ranges(organization_num_employees_ranges):
    try:
        # Regular expression to match the expected format
        if re.match(r'^\[(\d+,\d+)\](,\[(\d+,\d+)\])*?$',organization_num_employees_ranges):
            # Convert to a list of ranges
            ranges_list = [item.strip() for item in organization_num_employees_ranges.split(",")]
            # st.write("Parsed Ranges:", ranges_list)
            return True
        else:
            st.error("Invalid format for employee range! Please use the format: [1,10],[11,20],[21,50]")
            return False
    except Exception as e:
        execute_error_block(f"Error occured while validating the employee range. {e}")

def validate_job_seniorities(job_seniorities):
    try:
        if re.match(r'^(\w+\s*)*(,\s*\w+\s*\w*)*$',job_seniorities):
            ranges_list = [item.strip() for item in job_seniorities.split(",")]
            return True
        else:
            st.error("Invalid format for job seniorities! Please use the format: manager,director")
            return False
    except Exception as e:
        execute_error_block(f"Error occured while validating the job roles. {e}")

def validate_job_titles(job_titles):
    try:
        if re.match(r'^(\s*\w*\s*)*(,(\s*\w*\s*)*)*$',job_titles):
            ranges_list = [item.strip() for item in job_titles.split(",")]
            return True
        else:
            st.error("Invalid format for job titles! Please use the format: marketing manager,marketing director")
            return False
    except Exception as e:
        execute_error_block(f"Error occured while validating the job roles. {e}")

def validate_location_format(locations):
    try:
    
        if re.match(r'^\[(\s*\w*\s*)*(,(\s*\w*\s*)*)*\](,\[(\s*\w*\s*)*(,(\s*\w*\s*)*)*\])*$',locations):
            ranges_list = [item.strip() for item in locations.split(",")]
            return True
        else:
            st.error("Invalid format for location! Please use the format: [Dubai,United Arab Emirates],[Russia],[Quatar]")
            return False
    except Exception as e:
        execute_error_block(f"Error occured while validating the location format. {e}")

def validate_fields(job_titles,person_seniorities,person_locations,organization_locations,organization_num_employees_ranges):
    try:
        return validate_employee_ranges(organization_num_employees_ranges) and validate_location_format(person_locations) and validate_location_format(organization_locations) and validate_job_titles(job_titles) and validate_job_seniorities(person_seniorities) 
    except Exception as e:
        execute_error_block(f"Error occured while validating the fields. {e}")

# Title and Description
st.markdown('<div class="main-title">AI Persona Miner </div>', unsafe_allow_html=True)
# st.write("Use the filters below to customize the extraction of leads.")

# Filter Input
st.markdown('<div class="filters">', unsafe_allow_html=True)

job_titles = st.text_input("Job Titles (comma-separated):", "marketing manager,marketing director")
person_seniorities = st.text_input("Person Seniorities (comma-separated):", "owner,founder,director")
person_locations = st.text_input("Person Locations (Comma-separated list, Eg: [Dubai,United Arab Emirates],[India],[United States])", "[United States],[Germany],[France]")
organization_locations = st.text_input("Organization Locations (Comma-separated list, Eg: [Dubai,United Arab Emirates],[India],[United States])", "[United States],[Germany],[France]")
email_status = 'verified,likely to engage'
organization_num_employees_ranges = st.text_input("Organization Employee Ranges (Comma-separated list, Eg: [1,10],[11,20],[21,50])", "[1,10000000]")
page_number = st.text_input("Page Number:", 1)
results_per_page = st.text_input("Results per Page:", 1)
client_id = st.text_input("Client Id:", "berkleys_homes")
server_test = st.radio("Test in server:", options=["yes", "no"], index=1)
test_run = st.radio("Test run:", options=["yes", "no"], index=1)
qualify_leads = st.radio("Qualify Leads:", options=["yes", "no"], index=0)
test_run_id=''
if test_run=="yes":
    test_run_id = st.text_input("Test run id:", "63568114be7c760001dc78c6")

st.markdown('</div>', unsafe_allow_html=True)

if st.button("Fetch Data"):
    print('----------------Fields Validation--------------------')
    if validate_fields(
        job_titles=job_titles,
        person_seniorities=person_seniorities,
        person_locations=person_locations,
        organization_locations=organization_locations,
        organization_num_employees_ranges=organization_num_employees_ranges
        ):
        print('Success')
        st.write("Valid Input")

        if server_test == "yes":
            response = requests.get(
                    f"https://magmostafa.pythonanywhere.com/data_ingestion?page={page_number}&per_page={results_per_page}&job_titles={job_titles}&person_seniorities={person_seniorities}&person_locations={person_locations}&organization_locations={organization_locations}&email_status={email_status}&organization_num_employees_ranges={organization_num_employees_ranges}&client_id={client_id}&test_run_id={test_run_id}&qualify_leads={qualify_leads}"
                )
        else:
            response = requests.get(
                    f"http://127.0.0.1:5000/data_ingestion?page={page_number}&per_page={results_per_page}&job_titles={job_titles}&person_seniorities={person_seniorities}&person_locations={person_locations}&organization_locations={organization_locations}&email_status={email_status}&organization_num_employees_ranges={organization_num_employees_ranges}&client_id={client_id}&test_run_id={test_run_id}&qualify_leads={qualify_leads}"
                )
        print(response)
        print('-------COMPLETED-----------')
        if response.status_code == 200:
            st.write("API Call Successful!")
            # st.write(response.json())  # Display the response from the API
        else:
            st.write(response)
            st.error("Failed to fetch data.")

# Adding an image to the sidebar
st.sidebar.image(
    "logo.JPG",  
    caption="",  
    use_column_width=True
)

st.sidebar.markdown("<br>", unsafe_allow_html=True)  # Adds two blank lines

st.sidebar.markdown('<div class="sub-title">Filters Applied</div>', unsafe_allow_html=True)
st.sidebar.write("**Page Number:**", page_number)
st.sidebar.write("**Results per Page:**", results_per_page)
st.sidebar.write("**Job Titles:**", job_titles)
st.sidebar.write("**Person Seniorities:**", person_seniorities)
st.sidebar.write("**Person Locations:**", person_locations)
st.sidebar.write("**Organization Locations:**", organization_locations)
st.sidebar.write("**Email Status:**", email_status)
st.sidebar.write("**Organization Employee Ranges:**", organization_num_employees_ranges)

# Footer
st.markdown('<div class="footer">Generate | AI Powered Lead Generation System</div>', unsafe_allow_html=True)