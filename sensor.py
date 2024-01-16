import pika
import json
import logging
import csv
import time
import sys
import os

logging.basicConfig()

def send_messages(device_id):
    # Parse CLOUDAMQP_URL (fallback to localhost)
    url = url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
    params = pika.URLParameters(url)
    params.socket_timeout = 5

    connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
    channel = connection.channel()  # Start a channel

    # Declare the queue if it doesn't exist
    try:
        channel.queue_declare(queue='sensor_queue', durable=True)  # Assuming durable=True for persistence
        print(f"Queue 'sensor_queue' declared successfully.")
    except pika.exceptions.ChannelClosedByBroker:
        print(f"Queue 'sensor_queue' already exists.")

    # Read data from CSV file
    with open("./sensor.csv", newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            message_data = {
                "timestamp": int(time.time() * 1000),
                "device_id": device_id,
                "consumption": float(row['0'])
            }
            body = json.dumps(message_data)

            # Send each row as a message to the queue
            channel.basic_publish(exchange='', routing_key='sensor_queue', body=body, properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            ))
            print(f"[x] Message sent to consumer: {body}")

            time.sleep(5)

    # Close the connection
    connection.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    device_id = sys.argv[1]
    send_messages(device_id)