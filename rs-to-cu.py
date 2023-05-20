import requests
import sqlite3
from datetime import datetime, timedelta
import time
import random
import json
import logging
from flask import Flask, request, jsonify

(API KEYS GO HERE)

conn = sqlite3.connect('rs_cu_map.db')
cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS rs_cu_map (rs_ticket_number INTEGER, cu_task_id TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS rs_comments (comment_id INTEGER PRIMARY KEY, cu_task_id TEXT, cu_comment_id TEXT)")

REPAIRSHOPR_USERS = {
    59534: "Wayne Barahona",
    62231: "Sheldon-Clint Peterson",
    62235: "Sioux Center",
    91284: "Jon Hellinga",
    136438: "Nicholas Kats",
    157057: "Tanner Krull",
    165292: "Greg Lode",
    167714: "Alan Ruskauff",
    170396: "Mason Nerness",
    176665: "Jesus Uscanga",
    176816: "Jesse Espinoza",
    177425: "Taylor Reynolds"
}

def get_user_name(user_id, users_dict):
    return users_dict.get(user_id, 'Not specified')

def convert_datetime_format(date_str):
    dt_object = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt_object.strftime("%m-%d-%Y %I:%M %p")

def get_rs_ticket(api_key, ticket_id):
    url = f'https://superht.repairshopr.com/api/v1/tickets/{ticket_id}'
    headers = {'Authorization': f'Bearer {api_key}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()['ticket']
    else:
        return None
		
def find_matching_cu_task(rs_ticket_number):
    with sqlite3.connect('rs_cu_map.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT cu_task_id FROM rs_cu_map WHERE rs_ticket_number = ?", (rs_ticket_number,))
        result = cursor.fetchone()

    return result[0] if result else None

def create_cu_task(api_key, list_id, ticket, comments):
    url = f'https://api.clickup.com/api/v2/list/{list_id}/task'
    headers = {
        'Authorization': CLICKUP_API_KEY,
        'Content-Type': 'application/json'
    }
	# Sort comments by their 'created_at' field
    sorted_comments = sorted(ticket['comments'], key=lambda comment: comment['created_at'])

    # Format sorted comments
    comments_str = "\n\n".join(f"{get_user_name(comment['user_id'], REPAIRSHOPR_USERS)} | {comment['body']} | {datetime.fromisoformat(comment['created_at'].replace('Z', '+00:00')).strftime('%m-%d-%y - %I:%M %p')}" for comment in sorted_comments)
    print("Comments:", comments_str)
    data = {
        'name': ticket['subject'],
        'description': f"Problem Type: {ticket['problem_type']}\nStatus: {ticket['status']}\nCustomer: {ticket['customer_business_then_name']}\nDue Date: {ticket['due_date']}\nCreated At: {ticket['created_at']}\nRS User: {get_user_name(ticket['user_id'], REPAIRSHOPR_USERS)}\nComments: {comments_str}",
        'custom_fields': [
            {
                'id': CLICKUP_RS_USER_FIELD_ID,
                'value': get_user_name(ticket['user_id'], REPAIRSHOPR_USERS)
            },
            {
                'id': CLICKUP_RS_UPDATED_AT_FIELD_ID,
                'value': convert_datetime_format(ticket['updated_at'])
            },
            {
                'id': CLICKUP_RS_STATUS_FIELD_ID,
                'value': ticket['status']
            },
            {
                'id': CLICKUP_RS_TICKET_NUMBER_FIELD_ID,
                'value': ticket['number']
            },
            {
                'id': CLICKUP_CUSTOMER_FIELD_ID,
                'value': ticket['customer_business_then_name']
            },
            {
                'id': CLICKUP_CUSTOM_FIELD_TICKET_ID,
                'value': str(ticket['id'])
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)
    response_json = response.json()
    print(f"ClickUp API response: {response_json}")
    if response.status_code != 200:
        print(f"Failed to create task. Status code: {response.status_code}, Response: {response.text}")
        return

    if 'id' not in response_json:
        print("Unexpected response from ClickUp API. 'id' not found.")
        return

    with sqlite3.connect('rs_cu_map.db') as conn:
        cursor = conn.cursor()
        try:
            cu_task_id = response_json['id']
            if cu_task_id:
                # Save the task mapping
                cursor.execute("INSERT INTO rs_cu_map (rs_ticket_number, cu_task_id) VALUES (?, ?)", (ticket['number'], cu_task_id))
                conn.commit()

                for comment in comments:
                    add_comment_to_cu_task(CLICKUP_API_KEY, cu_task_id, comment['text'])
                    # Add the comment to the database
                    cursor.execute("INSERT INTO rs_comments (comment_id, cu_task_id) VALUES (?, ?)", (comment['id'], cu_task_id))
                    conn.commit()

        except KeyError:
            print("Could not find 'id' in ClickUp API response.")
            return
def get_comments_from_cu_task(api_key, task_id):
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}/comment", headers=headers)

    if response.status_code == 200:
        return response.json()['comments']
    else:
        print(f"Failed to fetch comments from task_id {task_id}: {response.status_code}, {response.text}")
        return []

def update_cu_task(api_key, task_id, ticket, comments):
    print(f"Updating task: {task_id}")
    url = f'https://api.clickup.com/api/v2/task/{task_id}'
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    # Sort comments by their 'created_at' field
    sorted_comments = sorted(ticket['comments'], key=lambda comment: comment['created_at'])

    # Format sorted comments
    comments_str = "\n\n".join(f"{get_user_name(comment['user_id'], REPAIRSHOPR_USERS)} | {comment['body']} | {datetime.fromisoformat(comment['created_at'].replace('Z', '+00:00')).strftime('%m-%d-%y - %I:%M %p')}" for comment in sorted_comments)

    data = {
        'name': ticket['subject'],
        'description': f"Problem Type: {ticket['problem_type']}\nStatus: {ticket['status']}\nCustomer: {ticket['customer_business_then_name']}\nDue Date: {ticket['due_date']}\nCreated At: {ticket['created_at']}\nRS User: {get_user_name(ticket['user_id'], REPAIRSHOPR_USERS)}\nComments: {comments_str}",
        'custom_fields': [
            {
                'id': CLICKUP_RS_USER_FIELD_ID,
                'value': get_user_name(ticket['user_id'], REPAIRSHOPR_USERS)
            },
            {
                'id': CLICKUP_RS_UPDATED_AT_FIELD_ID,
                'value': convert_datetime_format(ticket['updated_at'])
            },
            {
                'id': CLICKUP_RS_STATUS_FIELD_ID,
                'value': ticket['status']
            },
            {
                'id': CLICKUP_RS_TICKET_NUMBER_FIELD_ID,
                'value': str(ticket['number'])
            },
            {
                'id': CLICKUP_CUSTOMER_FIELD_ID,
                'value': ticket['customer_business_then_name']
            },
            {
                'id': CLICKUP_CUSTOM_FIELD_TICKET_ID,
                'value': str(ticket['id'])
            }
        ]
    }
    # Check if the ticket is resolved
    if ticket['status'].lower() == 'resolved':
        # Update the task status to closed in ClickUp
        data['status'] = 'complete'  # Assuming 'closed' is the status for closed tasks in ClickUp
    response = requests.put(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Failed to update task. Status code: {response.status_code}, Response: {response.text}")
    # Update the 'RS updated at' custom field
    data['custom_fields'].append({
            'id': CLICKUP_RS_UPDATED_AT_FIELD_ID,
            'value': convert_datetime_format(ticket['updated_at'])
        })
    with sqlite3.connect('rs_cu_map.db') as conn:
        cursor = conn.cursor()
        # Get the existing comments for this task
        existing_comment_ids = set()
        cursor.execute("SELECT comment_id FROM rs_comments WHERE cu_task_id = ?", (task_id,))
        for row in cursor.fetchall():
            existing_comment_ids.add(row[0])

    # Get the existing comments for this task from ClickUp
    cu_existing_comments = get_comments_from_cu_task(api_key, task_id)
    cu_existing_comment_texts = {comment['comment_text'] for comment in cu_existing_comments}
	
    for comment in comments:
        # Check if this comment has already been added to this task
        if comment['id'] in existing_comment_ids:
            print(f"Skipping comment {comment['id']} for task {task_id} as it already exists in the database.")
        elif comment['text'] in cu_existing_comment_texts:
            print(f"Skipping comment {comment['id']} for task {task_id} as it already exists in ClickUp.")
            # Add this comment to the database
            try:
                cursor.execute("INSERT INTO rs_comments (comment_id, cu_task_id) VALUES (?, ?)", (comment['id'], task_id))
                conn.commit()
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: rs_comments.comment_id' in str(e):
                    print(f"Skipping comment {comment['id']} for task {task_id} as it already exists in the database.")
                else:
                    raise e
            else:
                # Commit the transaction only if no exception was raised
                conn.commit()
        else:
            # Here we need to handle the case where a new comment needs to be added to the task in ClickUp
            # You need to implement the `add_comment_to_cu_task` function
            add_comment_to_cu_task(api_key, task_id, comment['text'])
def add_comment_to_cu_task(api_key, task_id, comment_text):
    print(f"Adding comment to task: {task_id}")
    url = f"https://api.clickup.com/api/v2/task/{task_id}/comment"
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    data = {
        'comment_text': comment_text
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Failed to add comment to task. Response: {response.text}")
    else:
        print(f"Successfully added comment to task.")
    response.raise_for_status()
    time.sleep(random.uniform(1.0, 2.0))  # wait for a random period between 1 and 2 seconds to avoid rate limiting
    return response
def find_rs_ticket_number_by_cu_task_id(cu_task_id):
    with sqlite3.connect('rs_cu_map.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT rs_ticket_number FROM rs_cu_map WHERE cu_task_id = ?", (cu_task_id,))
        result = cursor.fetchone()
    return result[0] if result else None

def update_rs_ticket_status(api_key, ticket_id, status):
    print(f"Attempting to update RepairShopr ticket {ticket_id} status to {status}")
    url = f'https://superht.repairshopr.com/api/v1/tickets/{ticket_id}'
    headers = {'Authorization': f'Bearer {api_key}'}
    data = {'status': status}
    response = requests.put(url, headers=headers, json=data)
    if response.status_code == 200:
        print(f"Successfully updated RepairShopr ticket {ticket_id} status to {status}")
        return response.json()['ticket']
    else:
        print(f"Failed to update RepairShopr ticket {ticket_id} status to {status}. Response code: {response.status_code}, Response body: {response.text}")
        return None

def sync_ticket_to_task(ticket_id):
    # Fetch the specific RS ticket
    rs_ticket = get_rs_ticket(REPAIRSHOPR_API_KEY, ticket_id)

    if rs_ticket is None:
        print(f"Could not find ticket with ID {ticket_id}")
        return

    matching_task_id = find_matching_cu_task(rs_ticket['number'])

    # Extract comments from the ticket
    comments = rs_ticket.get('comments', [])
    formatted_comments = []

    for comment in comments:
        comment_str = f"{comment['subject']} | {comment['body']} | {datetime.fromisoformat(comment['created_at'].replace('Z', '+00:00')).strftime('%m-%d-%y - %I:%M %p')}"
        formatted_comments.append({'id': comment['id'], 'text': comment_str})

    print(f"Fetched {len(formatted_comments)} comments for ticket {rs_ticket['id']}")

    if matching_task_id:
        update_cu_task(CLICKUP_API_KEY, matching_task_id, rs_ticket, formatted_comments)
    else:
        # Check if a task already exists in CU for this ticket
        existing_task_id = find_matching_cu_task(rs_ticket['number'])
        if existing_task_id:
            update_cu_task(CLICKUP_API_KEY, existing_task_id, rs_ticket, formatted_comments)
        else:
            create_cu_task(CLICKUP_API_KEY, CLICKUP_LIST_ID, rs_ticket, formatted_comments)
def get_rs_ticket_comments(api_key, ticket_id):
    url = f'https://superht.repairshopr.com/api/v1/tickets/{ticket_id}'
    headers = {'Authorization': f'Bearer {api_key}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        ticket = response.json()['ticket']
        return ticket.get('comments', [])
    else:
        print(f"Failed to get comments for ticket {ticket_id}. Status code: {response.status_code}, Response: {response.text}")
        return []

def sync_comments_to_repairshopr(cu_task_id, rs_ticket_id):
    conn = sqlite3.connect('rs_cu_map.db')
    cursor = conn.cursor()
    
    payload = request.get_json()

    if 'task_id' in payload and 'history_items' in payload:
        for item in payload['history_items']:
            if item['field'] == "comment":
                cu_task_id = payload['task_id']

                cu_task_comments = get_comments_from_cu_task(CLICKUP_API_KEY, cu_task_id)

                for comment in cu_task_comments:
                    # Check if comment is already in DB before processing
                    if not is_comment_in_db(comment['id']):
                        cursor.execute("INSERT INTO rs_comments (comment_id, cu_task_id) VALUES (?, ?)", (comment['id'], cu_task_id))

                        if rs_ticket_id is not None:
                            # Check if the comment was originally from RS
                            rs_comment_id = get_rs_comment_id_by_cu_comment_id(comment['id'])
                            if rs_comment_id is None:
                                # The comment was not originally from RS, so add it to RS
                                rs_comment_id = add_comment_to_rs_ticket(REPAIRSHOPR_API_KEY, rs_ticket_id, comment['comment_text'])
                                add_comment_to_db(rs_comment_id, comment['id'])
    
    conn.commit()
    conn.close()

def is_comment_in_db(comment_id):
    conn = sqlite3.connect('rs_cu_map.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM rs_comments WHERE cu_comment_id=?", (comment_id,))
    result = cursor.fetchone()
    conn.close()

    return result is not None

def add_comment_to_db(comment_id, cu_comment_id):
    conn = sqlite3.connect('rs_cu_map.db')
    cursor = conn.cursor()
    for _ in range(5):  # Retry up to 5 times
        try:
            cursor.execute("INSERT INTO rs_comments (comment_id, cu_comment_id) VALUES (?, ?)", (comment_id, cu_comment_id))
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e):
                print("Database is locked, retrying...")
                time.sleep(0.5)  # Wait for 0.5 seconds before retrying
            else:
                raise e
    else:
        print("Failed to add comment to database after 5 retries.")
    conn.close()

def get_rs_comment_id_by_cu_comment_id(cu_comment_id):
    with sqlite3.connect('rs_cu_map.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT comment_id FROM rs_comments WHERE cu_comment_id = ?", (cu_comment_id,))
        result = cursor.fetchone()

    return result[0] if result else None

def add_comment_to_rs_ticket(api_key, ticket_id, comment_text):
    url = f"https://superht.repairshopr.com/api/v1/tickets/{ticket_id}/comment"
    headers = {
        "accept": "application/json",
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    data = {
        "body": comment_text,
        "subject": "Update"
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        print("Comment successfully added to RepairShopr ticket")
    else:
        print("Failed to add comment to RepairShopr ticket")

app = Flask(__name__)

@app.errorhandler(500)
def internal_error(exception):
    app.logger.error(exception)
    return "500 error"
@app.route('/clickup_webhook', methods=['POST'])
def handle_clickup_webhook():
    try:
        payload = request.get_json()

        if 'task_id' in payload and 'history_items' in payload:
            cu_task_id = payload['task_id']
            # Get the task details from ClickUp API
            cu_task_details = get_cu_tasks(CLICKUP_API_KEY, cu_task_id)
            if cu_task_details is None:
                app.logger.error(f"Failed to get details for ClickUp task: {cu_task_id}")
                return jsonify({'message': 'Failed to get task details'}), 500

            # Extract the custom field value for the ticket ID
            rs_ticket_id = None
            if 'custom_fields' in cu_task_details:
                for custom_field in cu_task_details['custom_fields']:
                    if custom_field['id'] == CLICKUP_CUSTOM_FIELD_TICKET_ID:
                        rs_ticket_id = custom_field['value']
                        break

            if rs_ticket_id is not None:
                for item in payload['history_items']:
                    if item['field'] == "comment":
                        # Handle comment creation
                        sync_comments_to_repairshopr(cu_task_id, rs_ticket_id)
    except Exception as e:
        app.logger.exception("An error occurred during webhook processing: %s", e)
        return jsonify({'message': 'Webhook processing failed'}), 500

    return '', 200

@app.route('/sync', methods=['POST'])
def sync():
    app.logger.info(f"Received request: {request.json}")
    attributes = request.json.get('attributes')

    if attributes is None:
        app.logger.error("Missing attributes object in request")
        return {'message': 'Missing attributes object in request'}, 400

    # Check if ticket_id is present in the attributes
    ticket_id = attributes.get('ticket_id')

    # If ticket_id is not present, then it's probably a new ticket, so we'll use 'id'
    if ticket_id is None:
        ticket_id = attributes.get('id')
    
    if ticket_id is None:
        app.logger.error("Missing ticket id in request")
        return {'message': 'Missing ticket id in request'}, 400

    sync_ticket_to_task(ticket_id)
    return {'message': 'Sync started'}, 200


# Set up logging
handler = logging.FileHandler('server.log')
handler.setLevel(logging.ERROR)
app.logger.addHandler(handler)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
