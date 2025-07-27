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

load_dotenv()

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
APP_HOST = os.environ.get("APP_HOST", "0.0.0.0")
APP_PORT = os.environ.get("APP_PORT","8080")

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

@app.route('/')
def home():
    all_posts = []
    all_events_attendance = [] # Initialize
    return render_template(
    'index.html',
    posts=all_posts,
    all_events_attendance=all_events_attendance,)

@app.route('/hello')
def hello():
    return "Hello, World!"


if __name__ == '__main__':
    app.run(debug=True, host=APP_HOST, port=APP_PORT)