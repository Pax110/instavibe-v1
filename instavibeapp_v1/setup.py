import os
import uuid
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateutil_parser
import time
import json
from google.cloud import spanner
from google.api_core import exceptions

# --- Configuration ---
INSTANCE_ID = os.environ.get("SPANNER_INSTANCE_ID","instavibe-graph-instance-v1")
DATABASE_ID = os.environ.get("SPANNER_DATABASE_ID","graphdbv1")

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")

# --- Spanner Client Initialization ---
try:
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    print(f"Targeting Spanner: {instance.name}/databases/{database.name}")
    if not database.exists():
        print(f"Error: Database '{DATABASE_ID}' does not exist. Please create it first.")
        database = None
    else:
        print("Database connection successful.")
except exceptions.NotFound:
    print(f"Error: Spanner instance '{INSTANCE_ID}' not found or missing permissions.")
    spanner_client = None; instance = None; database = None
except Exception as e:
    print(f"Error initializing Spanner client: {e}")
    spanner_client = None; instance = None; database = None

def run_ddl_statements(db_instance, ddl_list, operation_description):
    """Helper function to run DDL statements and handle potential errors."""
    if not db_instance:
        print(f"Skipping DDL ({operation_description}) - database connection not available.")
        return False
    print(f"\n--- Running DDL: {operation_description} ---")
    print("Statements:")
    # Print statements cleanly
    for i, stmt in enumerate(ddl_list):
        print(f"  [{i+1}] {stmt.strip()}") # Add numbering for clarity
    try:
        operation = db_instance.update_ddl(ddl_list)
        print("Waiting for DDL operation to complete...")
        operation.result(360) # Wait up to 6 minutes
        print(f"DDL operation '{operation_description}' completed successfully.")
        return True
    except (exceptions.FailedPrecondition, exceptions.AlreadyExists) as e:
        print(f"Warning/Info during DDL '{operation_description}': {type(e).__name__} - {e}")
        print("Continuing script execution (schema object might already exist or precondition failed).")
        return True
    except exceptions.InvalidArgument as e:
        print(f"ERROR during DDL '{operation_description}': {type(e).__name__} - {e}")
        print(">>> This indicates a DDL syntax error. The schema was NOT created/updated correctly. Stopping script. <<<")
        return False # Make syntax errors fatal
    except exceptions.DeadlineExceeded:
        print(f"ERROR during DDL '{operation_description}': DeadlineExceeded - Operation took too long.")
        return False
    except Exception as e:
        print(f"ERROR during DDL '{operation_description}': {type(e).__name__} - {e}")
        # Optionally print full traceback for debugging
        import traceback
        traceback.print_exc()
        print("Stopping script due to unexpected DDL error.")
        return False

def setup_base_schema_and_indexes(db_instance):
    """Creates the base relational tables and associated indexes."""
    ddl_statements = [
        # --- 1. Base Tables (No Graph Definition Here) ---
        """
        CREATE TABLE IF NOT EXISTS Person (
            person_id STRING(36) NOT NULL,
            name STRING(MAX),
            age INT64,
            create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (person_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS Event (
            event_id STRING(36) NOT NULL,
            name STRING(MAX),
            description STRING(MAX), -- New field
            event_date TIMESTAMP,
            create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (event_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS Post (
            post_id STRING(36) NOT NULL,
            author_id STRING(36) NOT NULL, -- References Person.person_id
            text STRING(MAX),
            sentiment STRING(50),
            post_timestamp TIMESTAMP,
            create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (post_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS Friendship (
            person_id_a STRING(36) NOT NULL, -- References Person.person_id
            person_id_b STRING(36) NOT NULL, -- References Person.person_id
            friendship_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (person_id_a, person_id_b)
        """,
         """
        CREATE TABLE IF NOT EXISTS Attendance (
            person_id STRING(36) NOT NULL, -- References Person.person_id
            event_id STRING(36) NOT NULL,  -- References Event.event_id
            attendance_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (person_id, event_id)
        """,
            """
    CREATE TABLE IF NOT EXISTS Topic (
        topic_id STRING(36) NOT NULL,
        name STRING(200) NOT NULL,
        description STRING(MAX),
        create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
    ) PRIMARY KEY (topic_id)
    """,
        """
    CREATE TABLE TopicContent (
        topic_id STRING(36) NOT NULL,
        content_id STRING(36) NOT NULL,
        page_no INT64 NOT NULL,
        content_json JSON NOT NULL,
        create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
    ) PRIMARY KEY (topic_id, content_id),
        INTERLEAVE IN PARENT Topic ON DELETE CASCADE
    """,        
        """
        CREATE TABLE IF NOT EXISTS Mention (
            post_id STRING(36) NOT NULL,            -- References Post.post_id
            mentioned_person_id STRING(36) NOT NULL,-- References Person.person_id
            mention_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (post_id, mentioned_person_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS Location (
            location_id STRING(36) NOT NULL,
            name STRING(MAX),
            description STRING(MAX),
            latitude FLOAT64,
            longitude FLOAT64,
            address STRING(MAX),
            create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true)
        ) PRIMARY KEY (location_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS EventLocation (
            event_id STRING(36) NOT NULL,    -- References Event.event_id
            location_id STRING(36) NOT NULL, -- References Location.location_id
            create_time TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true),
            CONSTRAINT FK_Event FOREIGN KEY (event_id) REFERENCES Event (event_id),
            CONSTRAINT FK_Location FOREIGN KEY (location_id) REFERENCES Location (location_id)
        ) PRIMARY KEY (event_id, location_id)
        """,
        # --- 2. Indexes ---
        "CREATE INDEX IF NOT EXISTS PersonByName ON Person(name)",
        "CREATE INDEX IF NOT EXISTS EventByDate ON Event(event_date DESC)",
        "CREATE INDEX IF NOT EXISTS PostByTimestamp ON Post(post_timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS PostByAuthor ON Post(author_id, post_timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS FriendshipByPersonB ON Friendship(person_id_b, person_id_a)",
        "CREATE INDEX IF NOT EXISTS AttendanceByEvent ON Attendance(event_id, person_id)",
        "CREATE INDEX IF NOT EXISTS MentionByPerson ON Mention(mentioned_person_id, post_id)",
        "CREATE INDEX IF NOT EXISTS EventLocationByLocationId ON EventLocation(location_id, event_id)", # Index for linking table
        "CREATE UNIQUE INDEX IF NOT EXISTS TopicByName ON Topic(name)",
        "CREATE INDEX IF NOT EXISTS TopicContentByPage ON TopicContent(topic_id, page_no)",
    ]
    return run_ddl_statements(db_instance, ddl_statements, "Create Base Tables and Indexes")

# --- NEW: Function to create the property graph ---
def setup_graph_definition(db_instance):
    """Creates the Property Graph definition based on existing tables."""
    # NOTE: Graph name cannot contain hyphens if unquoted. Using SocialGraph.
    ddl_statements = [
        # --- Create the Property Graph Definition (Using SOURCE/DESTINATION) ---
        # "DROP PROPERTY GRAPH IF EXISTS SocialGraph", # Optional for dev
        """
        CREATE PROPERTY GRAPH IF NOT EXISTS SocialGraph
          NODE TABLES (
            Person KEY (person_id),
            Event KEY (event_id),
            Post KEY (post_id),
            Location KEY (location_id) -- New Node Table
          )
          EDGE TABLES (
            Friendship 
              SOURCE KEY (person_id_a) REFERENCES Person (person_id)
              DESTINATION KEY (person_id_b) REFERENCES Person (person_id),

            
            Attendance AS Attended 
              SOURCE KEY (person_id) REFERENCES Person (person_id)
              DESTINATION KEY (event_id) REFERENCES Event (event_id),

            
            Mention AS Mentioned
              SOURCE KEY (post_id) REFERENCES Post (post_id)
              DESTINATION KEY (mentioned_person_id) REFERENCES Person (person_id),

            
            Post AS Wrote 
              SOURCE KEY (author_id) REFERENCES Person (person_id)
              DESTINATION KEY (post_id) REFERENCES Post (post_id),

            EventLocation AS HasLocation -- New Edge Table
              SOURCE KEY (event_id) REFERENCES Event (event_id)
              DESTINATION KEY (location_id) REFERENCES Location (location_id)
          )
        """
    ]
    return run_ddl_statements(db_instance, ddl_statements, "Create Property Graph Definition")

    

# --- Data Generation / Insertion ---
def generate_uuid(): return str(uuid.uuid4())

def insert_relational_data(db_instance):
    """Generates and inserts the curated data into the new relational tables."""
    if not db_instance: print("Skipping data insertion - db connection unavailable."); return False
    print("\n--- Defining Fixed Curated Data for Relational Insertion ---")

    people_map = {} # name -> id
    event_map = {}  # name -> id
    # post_map is not strictly needed if we don't refer back to posts by internal ref later
    locations_map = {} # (name, lat, lon) -> location_id to avoid duplicate locations

    people_rows = []
    events_rows = []
    posts_rows = []
    friendship_rows = []
    attendance_rows = []
    mention_rows = []
    locations_rows = [] # For Location table
    event_locations_rows = [] # For EventLocation table
    topic_rows = []            # NEW
    topic_content_rows = []    # NEW
    topic_map = {}             # name -> topic_id

    now = datetime.now(timezone.utc)

    # 1. Prepare People Data
    people_data = {
        "Alice": {"age": 30}, "Bob": {"age": 28}, "Charlie": {"age": 35}, "Diana": {"age": 29},
        "Ethan": {"age": 31}, "Fiona": {"age": 27}, "George": {"age": 40}, "Hannah": {"age": 33},
        "Ian": {"age": 25}, "Julia": {"age": 38}, "Kevin": {"age": 22}, "Laura": {"age": 45},
        "Mike": {"age": 36}, "Nora": {"age": 29}, "Oscar": {"age": 32}
    }
    print(f"Preparing {len(people_data)} people.")
    for name, data in people_data.items():
        person_id = generate_uuid()
        people_map[name] = person_id
        people_rows.append({
            "person_id": person_id, "name": name, "age": data.get("age"), # Use .get for safety
            "create_time": spanner.COMMIT_TIMESTAMP
        })

    # 2. Prepare Events Data
    event_data = {
        "Charity Bake Sale": {"date": (now - timedelta(days=6, hours=4)).isoformat(), "description": "Support local charities by buying delicious baked goods. All proceeds go to a good cause.", "locations": [{"name": "Community Hall - Main Room", "description": "Cakes, pies, and cookies.", "latitude": 34.052235, "longitude": -118.243683, "address": "123 Main St, Anytown"}, {"name": "Community Hall - Patio", "description": "Brownies and beverages.", "latitude": 34.052000, "longitude": -118.243500, "address": "123 Main St, Anytown (Patio)"}]},
        "Tech Meetup: Future of AI": {"date": (now - timedelta(days=5, hours=10)).isoformat(), "description": "A deep dive into the future of Artificial Intelligence, with guest speakers from leading tech companies.", "locations": [{"name": "Innovation Hub Auditorium", "description": "Main presentations and Q&A.", "latitude": 37.774929, "longitude": -122.419418, "address": "456 Tech Ave, San Francisco"}]},
        "Central Park Picnic": {"date": (now - timedelta(days=4, hours=6)).isoformat(), "description": "A casual picnic in the park. Bring your own food and blankets!", "locations": [{"name": "Great Lawn - North End", "description": "Look for the blue balloons.", "latitude": 40.782864, "longitude": -73.965355, "address": "Central Park, New York"}]},
        "Indie Film Screening": {"date": (now - timedelta(days=3, hours=12)).isoformat(), "description": "Screening of 'The Lighthouse Keeper', followed by a Q&A with the director.", "locations": [{"name": "Art House Cinema", "description": "Screen 2.", "latitude": 34.090000, "longitude": -118.360000, "address": "789 Movie Ln, Los Angeles"}]},
        "Neighborhood Potluck": {"date": (now - timedelta(days=2, hours=8)).isoformat(), "description": "Share your favorite dish with your neighbors. Fun for the whole family.", "locations": [{"name": "Greenwood Park Pavilion", "description": "Covered area near the playground.", "latitude": 47.606209, "longitude": -122.332069, "address": "101 Park Rd, Seattle"}]},
        "Escape Room: The Lost Temple": {"date": (now - timedelta(days=1, hours=5)).isoformat(), "description": "Can you solve the puzzles and escape the Lost Temple in 60 minutes?", "locations": [{"name": "Enigma Escapes", "description": "The Lost Temple room.", "latitude": 30.267153, "longitude": -97.743057, "address": "321 Puzzle Pl, Austin"}]},
        "Music in the Park Festival": {"date": (now - timedelta(days=0, hours=18)).isoformat(), "description": "A two-day music festival featuring local bands and artists across multiple stages.", "locations": [{"name": "Main Stage - Meadow", "description": "Headline acts.", "latitude": 34.0600, "longitude": -118.2500, "address": "City Park, Meadow Area"}, {"name": "Acoustic Tent - By The Lake", "description": "Intimate performances.", "latitude": 34.0615, "longitude": -118.2520, "address": "City Park, Lakeside"}, {"name": "Food Truck Alley - East Path", "description": "Various food vendors.", "latitude": 34.0590, "longitude": -118.2480, "address": "City Park, East Pathway"}]}
    }
    print(f"Preparing {len(event_data)} events.")
    for name, data in event_data.items():
        event_id = generate_uuid()
        event_map[name] = event_id
        try:
             ts_str = data.get("date")
             if not ts_str:
                 print(f"Warning: Missing date for event '{name}', skipping.")
                 continue
             ts = dateutil_parser.isoparse(ts_str)
             # Ensure it's timezone-aware (Spanner prefers UTC)
             if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
                 ts = ts.replace(tzinfo=timezone.utc) # Assume naive dates are UTC
             else:
                 ts = ts.astimezone(timezone.utc) # Convert aware dates to UTC

             events_rows.append({
                "event_id": event_id, "name": name, "description": data.get("description"), "event_date": ts,
                "create_time": spanner.COMMIT_TIMESTAMP
             })

             if "locations" in data and isinstance(data["locations"], list):
                for loc_detail in data["locations"]:
                    loc_key_tuple = (loc_detail["name"], loc_detail["latitude"], loc_detail["longitude"]) # Unique key for this location instance
                    
                    if loc_key_tuple not in locations_map:
                        location_id = generate_uuid()
                        locations_map[loc_key_tuple] = location_id
                        locations_rows.append({
                            "location_id": location_id,
                            "name": loc_detail["name"],
                            "description": loc_detail.get("description"),
                            "latitude": loc_detail["latitude"],
                            "longitude": loc_detail["longitude"],
                            "address": loc_detail.get("address"),
                            "create_time": spanner.COMMIT_TIMESTAMP
                        })
                    else:
                        location_id = locations_map[loc_key_tuple]
                    
                    event_locations_rows.append({
                        "event_id": event_id, "location_id": location_id, "create_time": spanner.COMMIT_TIMESTAMP
                    })
        except (TypeError, ValueError, OverflowError) as e: # Catch specific errors
            print(f"Warning: Could not parse date for event '{name}' (value: {data.get('date')}, error: {e}), skipping.")

             
    # 2. Prepare Topics Data
        topic_seed = {
        "Artificial Intelligence": {
            "description": "Fundamentals, history, and future directions of AI.",
            "pages": [
                {"title": "What is AI?", "body": "Artificial Intelligence (AI) refers ..."},
                {"title": "Main Techniques", "body": "Machine learning, deep learning ..."},
                {"title": "Ethical Considerations", "body": "Bias, transparency ..."}
            ]
        },
        "Baking & Pastries": {
            "description": "Recipes and science behind breads, cakes, and cookies.",
            "pages": [
                {"title": "Yeast vs Chemical Leavening", "body": "Yeast produces CO₂ ..."},
                {"title": "Perfecting Pie Crust", "body": "Keep everything cold ..."}
            ]
        },
        "Outdoor Activities": {
            "description": "Hiking, camping, and nature safety tips.",
            "pages": [
                {"title": "Day‑Hike Checklist", "body": "Water, snacks, map ..."},
                {"title": "Leave No Trace", "body": "Plan ahead, travel on durable surfaces ..."}
            ]
        },
        "Film & Cinema": {
            "description": "History, genres, and filmmaking techniques.",
            "pages": [
                {"title": "Lighting 101", "body": "Key light, fill light ..."},
                {"title": "Film Movements", "body": "German Expressionism, French New Wave ..."}
            ]
        },
        "Mindfulness & Wellness": {
            "description": "Practices for mental clarity and stress reduction.",
            "pages": [
                {"title": "Introduction to Mindfulness", "body": "Focus on the breath ..."},
                {"title": "Building a Routine", "body": "Start with 5 minutes each morning ..."}
            ]
        }
    }
    for t_name, t_info in topic_seed.items():
        tid = generate_uuid()
        topic_map[t_name] = tid
        topic_rows.append({
            "topic_id": tid,
            "name": t_name,
            "description": t_info["description"],
            "create_time": spanner.COMMIT_TIMESTAMP
        })
        for page_no, page_obj in enumerate(t_info["pages"], start=1):
            topic_content_rows.append({
                "topic_id": tid,
                "content_id": generate_uuid(),
                "page_no": page_no,
                "content_json": json.dumps(page_obj),
                "create_time": spanner.COMMIT_TIMESTAMP
            })
    # 3. Prepare Friendships Data
    friendship_data = [("Alice", "Bob"), ("Alice", "Charlie"), ("Alice", "Hannah"), ("Alice", "Fiona"), ("Bob", "Diana"), ("Bob", "Ian"), ("Charlie", "Diana"), ("Charlie", "Ethan"), ("Diana", "Fiona"), ("Ethan", "Fiona"), ("Ethan", "George"), ("Ethan", "Ian"), ("Fiona", "Hannah"), ("Fiona", "Julia"), ("Fiona", "Ian"), ("Fiona", "Kevin"), ("Fiona", "Laura"), ("Fiona", "Mike"), ("Fiona", "Nora"), ("Fiona", "Oscar"), ("George", "Hannah"), ("George", "Ian"), ("Hannah", "Julia"), ("Ian", "Kevin"), ("Julia", "Kevin"), ("Julia", "Laura"), ("Kevin", "Mike"), ("Laura", "Nora"), ("Mike", "Oscar"), ("Nora", "Oscar")] # Removed one ("Oscar", "Nora") from original list which was a duplicate pair after sorting
    unique_friendship_pairs = set()
    print(f"Preparing friendships from {len(friendship_data)} potential pairs.")
    for p1_name, p2_name in friendship_data:
        if p1_name in people_map and p2_name in people_map:
             id1, id2 = people_map[p1_name], people_map[p2_name]
             if id1 == id2: continue # Skip self-friendship
             # Ensure person_id_a is lexicographically smaller than person_id_b for consistent PK
             person_id_a, person_id_b = tuple(sorted((id1, id2)))
             if (person_id_a, person_id_b) not in unique_friendship_pairs:
                 friendship_rows.append({
                    "person_id_a": person_id_a, "person_id_b": person_id_b,
                    "friendship_time": spanner.COMMIT_TIMESTAMP
                 })
                 unique_friendship_pairs.add((person_id_a, person_id_b))
        else:
            print(f"Warning: Skipping friendship due to missing person ('{p1_name}' or '{p2_name}').")
    print(f"Prepared {len(friendship_rows)} unique friendship rows.")


    # 4. Prepare Attendance Data
    attendance_data = [("Alice", "Charity Bake Sale"), ("Alice", "Tech Meetup: Future of AI"), ("Bob", "Charity Bake Sale"), ("Bob", "Central Park Picnic"), ("Charlie", "Tech Meetup: Future of AI"), ("Diana", "Central Park Picnic"), ("Diana", "Indie Film Screening"), ("Ethan", "Tech Meetup: Future of AI"), ("Ethan", "Neighborhood Potluck"), ("Fiona", "Central Park Picnic"), ("Fiona", "Escape Room: The Lost Temple"), ("George", "Neighborhood Potluck"), ("George", "Escape Room: The Lost Temple"), ("Hannah", "Charity Bake Sale"), ("Hannah", "Indie Film Screening"), ("Ian", "Tech Meetup: Future of AI"), ("Ian", "Neighborhood Potluck"), ("Julia", "Central Park Picnic"), ("Julia", "Escape Room: The Lost Temple"), ("Kevin", "Indie Film Screening"), ("Laura", "Neighborhood Potluck")]
    print(f"Preparing {len(attendance_data)} attendance records.")
    for person_name, event_name in attendance_data:
        if person_name in people_map and event_name in event_map:
            attendance_rows.append({
                "person_id": people_map[person_name], "event_id": event_map[event_name],
                "attendance_time": spanner.COMMIT_TIMESTAMP
            })
        else:
            print(f"Warning: Skipping attendance record due to missing person ('{person_name}') or event ('{event_name}').")

    # 5. Prepare Posts and Mentions Data
    # --- PASTE FULL posts_data list here ---
    posts_data = [
    {"person": "Alice", "text": "Just attended an eye-opening talk on AI alignment. So much to think about.", "sentiment": "neutral", "mention": None, "days_ago": 7, "hours_ago": 20},
    {"person": "Alice", "text": "The climate tech startup pitches were inspiring today!", "sentiment": "positive", "mention": "Hannah", "days_ago": 9, "hours_ago": 9},
    {"person": "Alice", "text": "Web3 is confusing but fascinating. Still not sure I buy into all the hype.", "sentiment": "negative", "mention": "Julia", "days_ago": 2, "hours_ago": 8},
    {"person": "Alice", "text": "Learned about carbon capture technology. We need more investment here.", "sentiment": "positive", "mention": "Fiona", "days_ago": 5, "hours_ago": 13},
    {"person": "Alice", "text": "Should we be worried about AI bias? The panel discussion was intense.", "sentiment": "negative", "mention": "Oscar", "days_ago": 6, "hours_ago": 22},
    {"person": "Alice", "text": "Been reading up on decentralized identity. Could be a game changer.", "sentiment": "neutral", "mention": None, "days_ago": 4, "hours_ago": 2},
    {"person": "Alice", "text": "Thinking of starting a side project involving smart contracts.", "sentiment": "positive", "mention": "Ethan", "days_ago": 0, "hours_ago": 4},
    {"person": "Alice", "text": "The ethics of surveillance tech in AI is really unsettling.", "sentiment": "negative", "mention": None, "days_ago": 10, "hours_ago": 17},
    {"person": "Alice", "text": "Crypto winter is rough, but I m holding on.", "sentiment": "neutral", "mention": "Charlie", "days_ago": 12, "hours_ago": 3},
    {"person": "Alice", "text": "Solar-powered desalination might solve major water issues. Promising!", "sentiment": "positive", "mention": None, "days_ago": 3, "hours_ago": 21},
    {"person": "Alice", "text": "DALL·E and Midjourney are impressive, but raise big questions about art.", "sentiment": "neutral", "mention": None, "days_ago": 1, "hours_ago": 14},
    {"person": "Alice", "text": "AI and healthcare—lots of potential, but privacy needs serious attention.", "sentiment": "negative", "mention": "Grace", "days_ago": 11, "hours_ago": 10},
    {"person": "Alice", "text": "Heard a great debate on NFT sustainability. Mixed feelings.", "sentiment": "neutral", "mention": None, "days_ago": 13, "hours_ago": 5},
    {"person": "Alice", "text": "Trying to understand zero-knowledge proofs. Brain is melting.", "sentiment": "negative", "mention": "Liam", "days_ago": 5, "hours_ago": 7},
    {"person": "Alice", "text": "Saw an amazing demo of drone reforestation. Tech meets ecology!", "sentiment": "positive", "mention": "Bob", "days_ago": 9, "hours_ago": 6},
    {"person": "Alice", "text": "Not sure if AI-generated resumes are ethical or not. Thoughts?", "sentiment": "neutral", "mention": None, "days_ago": 7, "hours_ago": 18},
    {"person": "Alice", "text": "Decentralized social media could reshape trust online.", "sentiment": "positive", "mention": "Nora", "days_ago": 6, "hours_ago": 12},
    {"person": "Alice", "text": "Learning about DAO governance models today. Some good, some chaotic.", "sentiment": "neutral", "mention": None, "days_ago": 8, "hours_ago": 9},
    {"person": "Alice", "text": "What if AI becomes sentient? Philosophical rabbit hole...", "sentiment": "neutral", "mention": None, "days_ago": 14, "hours_ago": 11},
    {"person": "Alice", "text": "EV battery recycling is the next frontier in green tech.", "sentiment": "positive", "mention": "Mike", "days_ago": 2, "hours_ago": 15},
    # ... continue with similar posts up to 100
]
 # <-- Make sure this contains the full list
    #---------------------------------------------

    print(f"Preparing {len(posts_data)} posts and associated mentions.")
    post_counter = 0
    for post_info in posts_data:
        person_name = post_info.get("person") # Assume this is correct
        if not person_name or person_name not in people_map:
            print(f"Warning: Skipping post from unknown or missing person '{person_name}': {post_info.get('text', 'N/A')[:50]}...")
            continue

        post_id = generate_uuid()
        post_counter += 1
        author_id = people_map[person_name]


        try:
            days_ago = post_info.get("days_ago", 0)
            hours_ago = post_info.get("hours_ago", 0)
            # Ensure timestamp is timezone-aware UTC
            post_timestamp = (now - timedelta(days=days_ago, hours=hours_ago))
            # No need to check tzinfo here as 'now' is already UTC
            # if post_timestamp.tzinfo is None:
            #      post_timestamp = post_timestamp.replace(tzinfo=timezone.utc)
            # else:
            #      post_timestamp = post_timestamp.astimezone(timezone.utc)

            posts_rows.append({
                "post_id": post_id,
                "author_id": author_id,
                "text": post_info.get("text"),
                "sentiment": post_info.get("sentiment"), # Use .get for safety
                "post_timestamp": post_timestamp,
                "create_time": spanner.COMMIT_TIMESTAMP
            })
        except (TypeError, ValueError, KeyError, OverflowError) as e:
            print(f"Warning: Skipping post due to data/time calculation issue ({e}): {post_info.get('text', 'N/A')[:50]}...")
            continue # Skip this post entirely if data is bad

        # Process mention only if post was successfully prepared
        mentioned_person_name = post_info.get("mention")
        if mentioned_person_name:
            if mentioned_person_name in people_map:
                mention_rows.append({
                    "post_id": post_id, # Use the generated post_id
                    "mentioned_person_id": people_map[mentioned_person_name],
                    "mention_time": spanner.COMMIT_TIMESTAMP # Use commit timestamp for simplicity
                })
            else:
                 print(f"Warning: Skipping mention for unknown person '{mentioned_person_name}' in post by '{person_name}'.")

    print(f"Prepared {len(posts_rows)} post rows, {len(mention_rows)} mention rows, {len(locations_rows)} location rows, and {len(event_locations_rows)} event-location link rows.")



    # --- 6. Insert Data into Spanner using a Transaction ---
    print("\n--- Inserting Data into Relational Tables ---")
    inserted_counts = {}

    # Define the function to be run in the transaction
    def insert_data_txn(transaction):
        total_rows_attempted = 0
        # Define structure: Table Name -> (Columns List, Rows Data List of Dicts)
        table_map = {
            "Person": (["person_id", "name", "age", "create_time"], people_rows),
            "Event": (["event_id", "name", "description", "event_date", "create_time"], events_rows),
            "Location": (["location_id", "name", "description", "latitude", "longitude", "address", "create_time"], locations_rows),
              # NEW: Topic parent before child
            "Topic": (["topic_id", "name", "description", "create_time"], topic_rows),
            "TopicContent": (["topic_id", "content_id", "page_no", "content_json", "create_time"], topic_content_rows),
            "Post": (["post_id", "author_id", "text", "sentiment", "post_timestamp", "create_time"], posts_rows),
            "Friendship": (["person_id_a", "person_id_b", "friendship_time"], friendship_rows),
            "Attendance": (["person_id", "event_id", "attendance_time"], attendance_rows),
            "Mention": (["post_id", "mentioned_person_id", "mention_time"], mention_rows),
            "EventLocation": (["event_id", "location_id", "create_time"], event_locations_rows)
        }

        for table_name, (cols, rows_dict_list) in table_map.items():
            if rows_dict_list:
                print(f"Inserting {len(rows_dict_list)} rows into {table_name}...")
                # Convert list of dicts into list of tuples matching column order
                values_list = []
                for row_dict in rows_dict_list:
                    try:
                        # Ensure all columns exist in the dict (or handle None)
                        # and are in the correct order
                        values_tuple = tuple(row_dict.get(c) for c in cols)
                        values_list.append(values_tuple)
                    except Exception as e:
                        print(f"Error preparing row for {table_name}: {e} - Row: {row_dict}")
                        # Decide if you want to skip this row or fail the transaction
                        # For now, let it potentially fail the transaction later if types mismatch etc.

                if values_list: # Only insert if we have valid rows prepared
                    transaction.insert(
                        table=table_name,
                        columns=cols,
                        values=values_list # Pass the list of tuples
                    )
                    inserted_counts[table_name] = len(values_list)
                    total_rows_attempted += len(values_list)
                else:
                    inserted_counts[table_name] = 0
            else:
                inserted_counts[table_name] = 0
        print(f"Transaction attempting to insert {total_rows_attempted} rows across all tables.")

    # Execute the transaction
    try:
        print("Executing data insertion transaction...")
        # Only run if there's actually data to insert
        all_data_lists = [people_rows, events_rows, locations_rows, posts_rows, friendship_rows,
                 attendance_rows, mention_rows, event_locations_rows, topic_rows, topic_content_rows]  # NEW lists included
        if any(len(data_list) > 0 for data_list in all_data_lists):
            db_instance.run_in_transaction(insert_data_txn)
            print("Transaction committed successfully.")
            for table, count in inserted_counts.items():
                if count > 0: print(f"  -> Inserted {count} rows into {table}.")
            return True
        else:
            print("No data prepared for insertion.")
        return True # Successful because nothing needed to be done
    except exceptions.Aborted as e:
         # Handle potential transaction aborts (e.g., contention) - retrying might be needed
         print(f"ERROR: Data insertion transaction aborted: {e}. Consider retrying.")
         return False
    except Exception as e:
        print(f"ERROR during data insertion transaction: {type(e).__name__} - {e}")
        # Optionally print more details for debugging complex errors
        import traceback
        traceback.print_exc()
        print("Data insertion failed. Database schema might exist but data is missing/incomplete.")
        return False


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Spanner Relational Schema Setup Script...")
    start_time = time.time()

    if not database:
        print("\nCritical Error: Spanner database connection not established. Aborting.")
        exit(1)

    # --- Step 1: Create schema (No Drops) ---
    # Added IF NOT EXISTS to CREATE INDEX statements for robustness
    if not setup_base_schema_and_indexes(database):
        print("\nAborting script due to errors during base schema/index creation.")
        exit(1)

    # --- Step 2: Create graph definition ---
    # Run this in a separate DDL operation
    if not setup_graph_definition(database):
        print("\nAborting script due to errors during graph definition creation.")
        exit(1)

    # --- Step 3: Insert data into the base tables ---
    if not insert_relational_data(database):
        print("\nScript finished with errors during data insertion.")
        exit(1)

    end_time = time.time()
    print("\n-----------------------------------------")
    print("Script finished successfully!")
    print(f"Database '{DATABASE_ID}' on instance '{INSTANCE_ID}' has been set up with the relational schema and populated.")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print("-----------------------------------------")
