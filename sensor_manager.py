import pika, os, time, subprocess

def manage_message(msg):
    device_id = int(msg)
    command = f"python sensor.py {device_id}"
    
    result = subprocess.Popen(command, shell=True)

    # Print the output
    print('Command Output:')
    print(result.stdout)

    # Print the error, if any
    if result.stderr:
        print('Command Error:')
        print(result.stderr)

    # Print the return code
    print('Return Code:', result.returncode)

    time.sleep(5)  # delays for 5 seconds


# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

# Declare the queue if it doesn't exist
try:
    channel.queue_declare(queue='device_id_queue', durable=True)  # Assuming durable=True for persistence
    print(f"Queue 'device_id_queue' declared successfully.")
except pika.exceptions.ChannelClosedByBroker:
    print(f"Queue 'device_id_queue' already exists.")

# create a function which is called on incoming messages
def callback(ch, method, properties, body):
  manage_message(body)

# set up subscription on the queue
channel.basic_consume('device_id_queue',
  callback,
  auto_ack=True)

# start consuming (blocks)
channel.start_consuming()
connection.close()