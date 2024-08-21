import azure.functions as func
import logging
import requests
import json
import os
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

storage_url = ""
container_name = ""  # Replace with your container name

# LLM Extractor function
def LLMExtractor(job_description, seniority, job_type, job_title, company_name):
    GPT4V_KEY = ""
    headers = {
        "Content-Type": "application/json",
        "api-key": GPT4V_KEY,
    }

    # Payload for the request
    payload = {
        # "response_format": { "type": "json_object" },
        "messages": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "You are an AI assistant that can classify a job description and extract useful information from it."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"I have a job description that I would like to classify and extract information from: {job_title}: {job_description} \n Seniority: {seniority} \n Job Type: {job_type} \n company name: {company_name}"
                    }
                ]
            }
        ],
        "temperature": 0.3,
        "top_p": 0.95,
        "max_tokens": 800,
        "functions": [
            {
                "name": "extract_info",
                "description": "Extract industry and company type of the company, function, domain, skills, and experience required from job description. Function - refers to the primary area or department within an organisation where the job role operates. It represents the broad category of work that the position falls under, indicating the main purpose or responsibility area of the role within the company structure.Domain - specifies the particular area of expertise or specialisation within the broader function. It represents a more focused subset of skills or knowledge required for the role, indicating the specific area of work or technology the position deals with on a day-to-day basis. Examples: (i) for Function - Engineering, Domain - Frontend or Backend or AI or QA or DevOps (ii) for Function - Finance, Domain - Auditing or Investor Relations (iii) for Function - Marketing, Domain - Content Strategy or Social Media or Performance Marketing or Ads",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "Industry": {"type": "string", "description": """The industry in which the company belongs, from the list {Aerospace & Defense, Agriculture, 
                                     Asset Management, Automotive, Banking, Capital Markets, Construction, Consumer Goods, Education, Energy & Utilities, Healthcare
                                     Technology,  Manufacturing, Media & Entertainment, Mining & Metals, Pharmaceuticals
                                     Real Estate, Retail, Telecommunications, Transportation & Logistics, Travel & Tourism}"""},
                        "Function": {"type": "string", "description": """The function of the job role, for example, electrical. From the list of functions: 
                                     {Engineering, Customer Service, Operations, Sales, Healthcare, Finance, Project Management, Human Resources, 
                                     Marketing, Administrative, Support Services, Research & Development, Design, Education, Safety & Quality, 
                                     Information Technology, Professional Services, Manufacturing, Legal & Risk Management, Consulting, Product Management, 
                                     Data Analysis, Environmental Services, Procurement, Supply Chain Management, Real Estate & Property Management, 
                                     Public Relations, Event Management, Art & Creative, Transport & Logistics, Food & Beverage Services}"""},
                        "Domain": {"type": "string", "description": "The domain, for example, Hardware, remember domain is a subset of function"},
                        "Company_Type": {"type": "string", "description": "The type of the company given the company name, from the list {Small and Medium Enterprises, Large Enterprises, Startups, Multinational Corporations} only"}
                        },
                    "required": ["Industry", "Function", "Domain"]
                }
            }
        ],
        "function_call": "auto"
    }

    GPT4V_ENDPOINT = ""
    # Send request
    try:
        response = requests.post(GPT4V_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response.json()  # Return the response as JSON
    except requests.RequestException as e:
        raise SystemExit(f"Failed to make the request. Error: {e}")    
    


# Upload to Azure Blob Storage
def upload_to_blob_storage(json_data, blob_name=None):    
    # Generate a unique blob name if not provided
    if not blob_name:
        blob_name = f"sample-blob-{str(uuid.uuid4())[0:5]}.json"
    
    # Create the BlobServiceClient and BlobClient objects
    blob_service_client = BlobServiceClient(account_url=storage_url, credential=DefaultAzureCredential())
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Convert dict to JSON string and upload
    blob_client.upload_blob(json.dumps(json_data, indent=4), overwrite=True)
    print(f"File {blob_name} uploaded to Azure Blob Storage.")


# # Create a hierarchical tree structure
def make_tree(list_of_dicts):
    tree = {}
    for dict_ in list_of_dicts:
        industry = dict_['Industry']
        function = dict_['Function']
        domain = dict_['Domain']
        seniority = dict_['seniority']
        
        if industry not in tree:
            tree[industry] = {}
        if function not in tree[industry]:
            tree[industry][function] = {}
        if domain not in tree[industry][function]:
            tree[industry][function][domain] = {}
        if seniority not in tree[industry][function][domain]:
            tree[industry][function][domain][seniority] = []
        
        if len(tree[industry][function][domain][seniority]) == 7:
            continue

        tree[industry][function][domain][seniority].append(dict_['job_description'])
    return tree

def read_from_blob(blob_name):
    # Create the BlobServiceClient and BlobClient objects
    blob_service_client = BlobServiceClient(account_url=storage_url, credential=DefaultAzureCredential())
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download the blob's content and parse it as JSON
    stream = blob_client.download_blob()
    return json.loads(stream.readall())


def process_batch(batch, batch_number):
    results = []
    # iterate over the batch
    for jobs, i in enumerate(batch):
        job_description = jobs['description']
        seniority = jobs['seniority']
        company_name = jobs['company_name']
        job_type = jobs['employment_type']
        job_title = jobs['title']
        if i % 100 == 0:
            time.sleep(1)

        extracted_info = LLMExtractor(job_description, seniority, job_type, job_title, company_name)
        if extracted_info and 'choices' in extracted_info and len(extracted_info['choices']) > 0:
            function_call_result = extracted_info['choices'][0]['message']['function_call']['arguments']
            extracted_dict = json.loads(function_call_result)
            extracted_dict['job_title'] = job_title
            extracted_dict['seniority'] = seniority
            extracted_dict['company_name'] = company_name
            extracted_dict['job_type'] = job_type
            extracted_dict['job_description'] = job_description
            results.append(extracted_dict)
    print(f"Batch {batch_number} processing completed.")
    return results

def main(dataset_dict, batch_size=20):
    assert batch_size > 0, "Batch size must be greater than 0."
    # open jobs.json
    jobs = dataset_dict

    num_batches = len(jobs) // batch_size + (1 if len(jobs) % batch_size != 0 else 0)
    results = []
    for batch_number in range(num_batches):
        batch_start = batch_number * batch_size
        batch_end = min((batch_number + 1) * batch_size, len(jobs))
        batch = jobs[batch_start:batch_end]
        results.extend(process_batch(batch, batch_number))

    return results


@app.route(route="function")
def function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    dataset = read_from_blob("jobs.json")
    result = main(dataset)
    tree = make_tree(result)
    upload_to_blob_storage(tree, "tree.json")
    
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # result = abcd()
        # result = LLMExtractor(job_description, name, job_title, company_name)
        func.HttpResponse(f"Extracting information from jd....")
        return func.HttpResponse(f"Extracted information from jd: {result}")
    
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.{result}",
             status_code=200
        )