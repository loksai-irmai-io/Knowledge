import logging
from neo4j import GraphDatabase
import json
import time
from datetime import datetime

# Load configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Neo4j connection details from config
URI = config["neo4j"]["URI"]
AUTH = (config["neo4j"]["username"], config["neo4j"]["password"])

# Batch size from config
BATCH_SIZE = config["batch_size"]

# Data file path from config
DATA_FILE = config["data_file"]

# Configure logging to write to graph.log
logging.basicConfig(filename="graph.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load JSON file
with open(DATA_FILE, "r") as file:
    json_data = json.load(file)

# Function to execute queries in batches
def execute_batch(driver, query, batch):
    if batch:
        with driver.session() as session:
            session.write_transaction(lambda tx: [tx.run(query, parameters=record) for record in batch])

# Function to log time for each step
def log_step(step_name):
    logger.info(f"Starting: {step_name}...")
    return time.time()

def log_completion(step_name, start_time):
    elapsed_time = time.time() - start_time
    logger.info(f"{step_name} completed in {elapsed_time:.2f} seconds.")

# Connect to Neo4j
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    logger.info("Starting full data upload process...")

    # **Step 1: Drop and Create Constraints/Indexes**
    start_time = log_step("Constraints and indexes creation")
    constraint_queries = [
        "DROP CONSTRAINT event_id_unique IF EXISTS;",
        "DROP CONSTRAINT case_unique IF EXISTS;",
        "DROP INDEX activity_name IF EXISTS;",

        # Create constraints and indexes
        "CREATE CONSTRAINT event_id_unique IF NOT EXISTS FOR (n:Event) REQUIRE n.ocel_id IS UNIQUE;",
        "CREATE CONSTRAINT case_unique IF NOT EXISTS FOR (n:Case) REQUIRE n.case_id IS UNIQUE;",
        "CREATE INDEX activity_name IF NOT EXISTS FOR (n:Activity) ON (n.name);"
    ]

    for query in constraint_queries:
        with driver.session() as session:
            session.run(query)
    log_completion("Constraints and indexes creation", start_time)

    # **Step 2: Insert Event Nodes**
    start_time = log_step("Event nodes insertion")
    event_query = """
        MERGE (e:Event {ocel_id: $id})
        SET e.timestamp = datetime($timestamp),
            e.activity_name = $activity
    """

    event_batch = []
    for event in json_data['ocel:events']:
        event_batch.append({
            "id": event['ocel:id'],
            "timestamp": event['ocel:timestamp'],
            "activity": event['ocel:activity']
        })
        if len(event_batch) >= BATCH_SIZE:
            execute_batch(driver, event_query, event_batch)
            event_batch = []
    execute_batch(driver, event_query, event_batch)
    log_completion("Event nodes insertion", start_time)

    # **Step 3: Insert Case Nodes**
    start_time = log_step("Case nodes insertion")
    case_query = """
        MATCH (e:Event {ocel_id: $id})
        MERGE (c:Case {case_id: $case_id})
        MERGE (c)-[:CONTAINS_EVENT]->(e)
    """

    case_batch = []
    for event in json_data['ocel:events']:
        if "case_id" in event["ocel:attributes"]:
            case_batch.append({
                "id": event['ocel:id'],
                "case_id": event['ocel:attributes']['case_id']
            })
        if len(case_batch) >= BATCH_SIZE:
            execute_batch(driver, case_query, case_batch)
            case_batch = []
    execute_batch(driver, case_query, case_batch)
    log_completion("Case nodes insertion", start_time)

    # **Step 4: Insert Activity Nodes**
    start_time = log_step("Activity nodes insertion")
    activity_query = """
        MATCH (e:Event {ocel_id: $id})
        MERGE (a:Activity {name: $activity, timestamp: $timestamp}) 
        MERGE (e)-[:OF_TYPE]->(a)
    """

    activity_batch = []
    for event in json_data['ocel:events']:
        activity_batch.append({
            "id": event['ocel:id'],
            "activity": event['ocel:activity'],
            "timestamp": event.get('ocel:timestamp', datetime.utcnow().isoformat())
        })
        if len(activity_batch) >= BATCH_SIZE:
            execute_batch(driver, activity_query, activity_batch)
            activity_batch = []
    if activity_batch:
        execute_batch(driver, activity_query, activity_batch)
    log_completion("Activity nodes insertion", start_time)

    # **Step 5: Insert Resource Nodes**
    start_time = log_step("Resource nodes insertion")
    resource_query = """
        MATCH (e:Event {ocel_id: $id})
        MERGE (r:Resource {name: $resource})
        MERGE (e)-[:PERFORMED_BY]->(r)
    """

    resource_batch = []
    for event in json_data['ocel:events']:
        if "resource" in event["ocel:attributes"]:
            resource_batch.append({
                "id": event['ocel:id'],
                "resource": event['ocel:attributes']['resource']
            })
        if len(resource_batch) >= BATCH_SIZE:
            execute_batch(driver, resource_query, resource_batch)
            resource_batch = []
    execute_batch(driver, resource_query, resource_batch)
    log_completion("Resource nodes insertion", start_time)

 # **Step 6: Insert Object Nodes**
    start_time = log_step("Object nodes insertion")
    object_query = """
        MATCH (e:Event {ocel_id: $id})
        MERGE (o:Object {id: $obj_id, type: $obj_type})
        MERGE (e)-[:INVOLVES]->(o)
    """

    object_batch = []
    for event in json_data['ocel:events']:
        for obj in event.get('ocel:objects', []):
            object_batch.append({
                "id": event['ocel:id'],
                "obj_id": obj["id"],
                "obj_type": obj["type"]
            })
        if len(object_batch) >= BATCH_SIZE:
            execute_batch(driver, object_query, object_batch)
            object_batch = []
    execute_batch(driver, object_query, object_batch)
    log_completion("Object nodes insertion", start_time)
    
# **Step 7: Create Sequence Relationships**
start_time = log_step("Sequence relationships creation")

sequence_query = """
    MATCH (e1:Event)-[:CONTAINS_EVENT]-(c:Case)
    WITH e1, c
    ORDER BY c.case_id, e1.timestamp
    WITH c, collect(e1) AS events
    UNWIND range(0, size(events)-2) AS i
    WITH events[i] AS e1, events[i+1] AS e2, c
    WHERE e1.timestamp < e2.timestamp AND e1 <> e2
    MERGE (e1)-[r:NEXT_EVENT]->(e2)
    SET r.time_difference = duration.inSeconds(e1.timestamp, e2.timestamp);
"""

with driver.session() as session:
    session.run(sequence_query)

log_completion("Sequence relationships creation", start_time)


logger.info("All data successfully uploaded to Neo4j AuraDB!")
