"""
    This program sends a message to a queue on the RabbitMQ server.
    Messages come from a csv file of shipment modes and costs.
    Messages are sent every 5 seconds.
    If an excessively priced shipment (one costing $100,000 or more) is detected, and extra alert message is sent to the consumer.


    Author: Zach Fuller
    Date: September 9, 2023

"""

import pika
import sys
import webbrowser
import csv
import time
import logging



# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Declare program constants
HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)
SHIPPING_FILE_NAME = "shipping.csv"
SHOW_OFFER = True  # Control whether to show the RabbitMQ Admin webpage offer

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    global SHOW_OFFER
    if SHOW_OFFER:
        webbrowser.open_new("http://localhost:15672/#/queues")


def send_message(host: str, queue_name: str, shipment_mode: str, shipment_cost: float):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        shipment_mode (str): the shipment mode, as read from the first column in the CSV file
        shipment_cost (float): the shipment cost, as read from the second column in the CSV file
    """
    # set the variable for the message to be sent
    # if the cost of the shipment is >= $100k, add an extra message regarding the excessive cost
    if shipment_cost >= float(100000):
        message = f"EXCESSIVE COST ALERT! A shipment was sent by {shipment_mode} for ${shipment_cost}."
    else:
        message = f"A shipment was sent by {shipment_mode} for ${shipment_cost}."
        
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        logging.info(f"{shipment_mode} shipment sent.")
        # send message every 5 seconds
        time.sleep(5)
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


def read_shipments_from_csv(file_name):
    """Read shipments from a CSV file and return them as a list."""
    with open(file_name, "r") as input_file:
        reader = csv.reader(input_file)
        next(reader) # skip header
        for row in reader:
            if row:
                shipment_mode = row[0] # read first row of CSV to store shipment mode
                shipment_cost = round(float(row[1]), 2) # read second row of CSV to store cost, make sure it is stored as a float, and round to 2 decimal places
                send_message("localhost", "shipping_queue", shipment_mode, shipment_cost)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()

    # call the custom function to read shipments from csv to look at our previously specified csv file
    # send_message is called as part of the read_shipments_from_csv function
    read_shipments_from_csv(SHIPPING_FILE_NAME)