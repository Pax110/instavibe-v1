import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, render_template, abort, flash, request, jsonify
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.api_core import exceptions
import humanize 
import uuid
import traceback
from dateutil import parser 


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a_default_secret_key_for_dev") 


load_dotenv()

INSTANCE_ID = "instavibe-graph-instance-v1" # Replace if different
DATABASE_ID = "graphdbv1" # Replace if different
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
APP_HOST = os.environ.get("APP_HOST", "0.0.0.0")
APP_PORT = os.environ.get("APP_PORT","8080")


# --- Spanner Client Initialization ---
db = None
try:
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    print(f"Attempting to connect to Spanner: {instance.name}/databases/{database.name}")

    # Ensure database exists - crucial check
    if not database.exists():
         print(f"Error: Database '{database.name}' does not exist in instance '{instance.name}'.")
         print("Please create the database and the required tables/schema.")
         # You might want to exit or handle this more gracefully depending on deployment
         # For now, we'll let it fail later if db is None
    else:
        print("Database connection check successful (database exists).")
        db = database # Assign database object if it exists

except exceptions.NotFound:
    print(f"Error: Spanner instance '{INSTANCE_ID}' not found in project '{PROJECT_ID}'.")
    # Handle error appropriately - exit, default behavior, etc.
except Exception as e:
    print(f"An unexpected error occurred during Spanner initialization: {e}")
    # Handle error


@app.template_filter('humanize_datetime')
def _jinja2_filter_humanize_datetime(value, default="just now"):
    """
    Convert a datetime object to a human-readable relative time string.
    e.g., '5 minutes ago', '2 hours ago', '3 days ago'
    """
    if not value:
        return default
   
    dt_object = None
    if isinstance(value, str):
        try:
            # Attempt to parse ISO 8601 format.
            # .replace('Z', '+00:00') handles UTC 'Z' suffix for fromisoformat.
            dt_object = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            # Fallback to dateutil.parser for more general string formats
            try:
                dt_object = parser.parse(value)
            except (parser.ParserError, TypeError, ValueError) as e:
                app.logger.warning(f"Could not parse date string '{value}' in humanize_datetime: {e}")
                return str(value) # Return original string if unparseable
    elif isinstance(value, datetime):
        dt_object = value
    else:
        # If not a string or datetime, return its string representation
        return str(value)

    if dt_object is None: # Should have been handled, but as a safeguard
        app.logger.warning(f"Date value '{value}' resulted in None dt_object in humanize_datetime.")
        return str(value)

    now = datetime.now(timezone.utc)
    # Use dt_object for all datetime operations from here
    if dt_object.tzinfo is None or dt_object.tzinfo.utcoffset(dt_object) is None:
        # If dt_object is naive, assume it's UTC
        dt_object = dt_object.replace(tzinfo=timezone.utc)
    else:
        # Convert aware dates to UTC
        dt_object = dt_object.astimezone(timezone.utc)

    try:
        return humanize.naturaltime(now - dt_object)
    except TypeError:
        # Fallback or handle error if date calculation fails
        return dt_object.strftime("%Y-%m-%d %H:%M")



def run_query(sql, params=None, param_types=None, expected_fields=None): # Add expected_fields
    """
    Executes a SQL query against the Spanner database.

    Args:
        sql (str): The SQL query string.
        params (dict, optional): Dictionary of query parameters. Defaults to None.
        param_types (dict, optional): Dictionary mapping parameter names to their
                                      Spanner types (e.g., spanner.param_types.STRING).
                                      Defaults to None.
        expected_fields (list[str], optional): A list of strings representing the
                                                expected column names in the order
                                                they appear in the SELECT statement.
                                                Required if results.fields fails.
    """
    if not db:
        print("Error: Database connection is not available.")
        raise ConnectionError("Spanner database connection not initialized.")

    results_list = []
    print(f"--- Executing SQL ---")
    print(f"SQL: {sql}")
    if params:
        print(f"Params: {params}")
    print("----------------------")

    try:
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(
                sql,
                params=params,
                param_types=param_types
            )

            # --- MODIFICATION START ---
            # Define field names based on the expected_fields argument
            # This avoids accessing results.fields which caused the error
            field_names = expected_fields
            if not field_names:
                 # Fallback or raise error if expected_fields were not provided
                 # For now, let's try the potentially failing way if not provided
                 print("Warning: expected_fields not provided to run_query. Attempting dynamic lookup.")
                 try:
                     field_names = [field.name for field in results.fields]
                 except AttributeError as e:
                     print(f"Error accessing results.fields even as fallback: {e}")
                     print("Cannot process results without field names.")
                     # Decide: raise error or return empty list?
                     raise ValueError("Could not determine field names for query results.") from e


            print(f"Using field names: {field_names}")
            # --- MODIFICATION END ---

            for row in results:
                # Now zip the known field names with the row values (which are lists)
                if len(field_names) != len(row):
                     print(f"Warning: Mismatch between number of field names ({len(field_names)}) and row values ({len(row)})")
                     print(f"Fields: {field_names}")
                     print(f"Row: {row}")
                     # Skip this row or handle error appropriately
                     continue # Skip malformed row for now
                results_list.append(dict(zip(field_names, row)))

            print(f"Query successful, fetched {len(results_list)} rows.")

    except (exceptions.NotFound, exceptions.PermissionDenied, exceptions.InvalidArgument) as spanner_err:
        print(f"Spanner Error ({type(spanner_err).__name__}): {spanner_err}")
        flash(f"Database error: {spanner_err}", "danger")
        return []
    except ValueError as e: # Catch the ValueError we might raise above
         print(f"Query Processing Error: {e}")
         flash("Internal error processing query results.", "danger")
         return []
    except Exception as e:
        print(f"An unexpected error occurred during query execution or processing: {e}")
        traceback.print_exc()
        flash(f"An unexpected server error occurred while fetching data.", "danger")
        raise e

    return results_list

# --- HOW TO CALL IT ---

def get_all_posts_with_author_db():
    """Fetch all posts and join with author information from Spanner."""
    sql = """
        SELECT
            p.post_id, p.author_id, p.text, p.sentiment, p.post_timestamp,
            author.name as author_name
        FROM Post AS p
        JOIN Person AS author ON p.author_id = author.person_id
        ORDER BY p.post_timestamp DESC
    """
    # Define the fields exactly as they appear in the SELECT statement
    fields = ["post_id", "author_id", "text", "sentiment", "post_timestamp", "author_name"]
    return run_query(sql, expected_fields=fields) # Pass the list here


def get_all_topics():
    """Fetch all topics from Spanner."""
    sql = """
        SELECT
        topic_id,
        name,
        description,
        create_time
        FROM `Topic`
        ORDER BY name
    """
    # Define the fields exactly as they appear in the SELECT statement
    fields = ["topic_id", "name", "description", "create_time"]
    return run_query(sql, expected_fields=fields) # Pass the list here

def get_all_events_with_attendees_db():
    """Fetch all events and their attendees from Spanner."""
    # Get all events first
    event_sql = """
        SELECT event_id, name, event_date
        FROM Event
        ORDER BY event_date DESC
        LIMIT 50
    """
    event_fields = ["event_id", "name", "event_date"]
    events = run_query(event_sql, expected_fields=event_fields)
    if not events:
        return []

    events_with_attendees = {event['event_id']: {'details': event, 'attendees': []} for event in events}
    event_ids = list(events_with_attendees.keys())

    # Fetch attendees
    attendee_sql = """
        SELECT
            a.event_id,
            p.person_id, p.name
        FROM Attendance AS a
        JOIN Person AS p ON a.person_id = p.person_id
        WHERE a.event_id IN UNNEST(@event_ids)
        ORDER BY a.event_id, p.name
    """
    params = {"event_ids": event_ids}
    param_types_map = {"event_ids": param_types.Array(param_types.STRING)}
    attendee_fields = ["event_id", "person_id", "name"]
    all_attendees = run_query(attendee_sql, params=params, param_types=param_types_map, expected_fields=attendee_fields)

    for attendee in all_attendees:
        event_id = attendee['event_id']
        if event_id in events_with_attendees:
            # No change needed here, attendee is already a dict
            events_with_attendees[event_id]['attendees'].append(attendee)

    return [events_with_attendees[event['event_id']] for event in events]

    
@app.route('/')
def home():
    all_posts = []
    all_events_attendance = [] # Initialize


    if not db:
        flash("Database connection not available. Cannot load page data.", "danger")
    else:
        try:
            # Fetch both posts and events
            all_posts = get_all_posts_with_author_db()
            all_events_attendance = get_all_events_with_attendees_db() # Fetch events
            topics = get_all_topics()
        except Exception as e:
             flash(f"Failed to load page data: {e}", "danger")
             # Ensure variables are defined even on error
             all_posts = []
             all_events_attendance = []
    return render_template(
    'index.html',
    posts=all_posts,
    all_events_attendance=all_events_attendance,
    topics=topics
    )

@app.route('/hello')
def hello():
    return "Hello, World!"

@app.route("/event/<event_id>")
def event_detail_page(event_id):
    return f"Event page for {event_id}"

@app.route('/person/<string:person_id>')
def person_profile(person_id):
    """Person profile page, fetching data from Spanner."""
    return f"Person page for {person_id}"



if __name__ == '__main__':
    app.run(debug=True, host=APP_HOST, port=APP_PORT)