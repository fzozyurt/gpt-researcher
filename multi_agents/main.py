from dotenv import load_dotenv
import sys
import os
import uuid
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from multi_agents.agents import ChiefEditorAgent
import asyncio
import json
from gpt_researcher.utils.enum import Tone

from kafka import KafkaConsumer

# Run with LangSmith if API key is set
if os.environ.get("LANGCHAIN_API_KEY"):
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
load_dotenv()

def open_task(json_data=None):
    if json_data:
        task = json.loads(json_data)
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        task_json_path = os.path.join(current_dir, 'task.json')
        with open(task_json_path, 'r') as f:
            task = json.load(f)

    if not task:
        raise Exception("No task found. Please ensure a valid task.json file is present in the multi_agents directory and contains the necessary task information.")

    return task

async def run_research_task(query, websocket=None, stream_output=None, tone=Tone.Objective, headers=None):
    task = open_task()
    task["query"] = query

    chief_editor = ChiefEditorAgent(task, websocket, stream_output, tone, headers)
    research_report = await chief_editor.run_research_task()

    if websocket and stream_output:
        await stream_output("logs", "research_report", research_report, websocket)

    return research_report
    
async def process_kafka_message(message):
    task = open_task(message.value.decode('utf-8'))
    chief_editor = ChiefEditorAgent(task)
    research_report = await chief_editor.run_research_task(task_id=uuid.uuid4())
    return research_report
    
async def main():
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_topic = os.environ.get('KAFKA_TOPIC', 'your_topic')
        kafka_group_id = os.environ.get('KAFKA_GROUP_ID', 'your_group_id')
    
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=kafka_group_id
        )
    
        for message in consumer:
            research_report = await process_kafka_message(message)
            return research_report
            # handle research report (e.g., save to file, send to another service, etc.)
     else:
        task = open_task()
        chief_editor = ChiefEditorAgent(task)
        research_report = await chief_editor.run_research_task(task_id=uuid.uuid4())
        return research_report
        # handle research report (e.g., save to file, send to another service, etc.)

if __name__ == "__main__":
    asyncio.run(main())
