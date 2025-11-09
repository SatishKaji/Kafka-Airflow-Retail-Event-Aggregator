import json
import random
from datetime import datetime
from kafka import KafkaProducer

kafka_broker = 'kafka:29092'
kafka_topic = 'retail_events'
max_events = 10
product_price = {'P_101':100, 'P_102':150, 'P_103':500, 'P_104':900, 'P_105':820, 'P_106':20}

def current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def generate_users():
    return f"user_{random.randint(1,10)}"



def generate_products():
    return f"p_{random.randint(1,5)}"


def item_viewed():
    user = generate_users()
    product = random.choice(list(product_price.keys()))
    return {

        'event_type':'Item_Viewed',
        'timestamp' : current_time(),
        'user_id' : user,
        'product_id' : product

    }



def item_added_to_cart():
    user = generate_users()
    product = random.choice(list(product_price.keys()))
    return {

        'event_type':'Added_to_Cart',
        'timestamp' : current_time(),
        'user_id' : user,
        'product_id' : product,
        'quantity': random.randint(1, 3),
    }


def order_created():
    user = generate_users()
    product = random.choice(list(product_price.keys()))
    return {

        'event_type':'Order_Created',
        'timestamp' : current_time(),
        'user_id' : user,
        'product_id' : product,
        'quantity': random.randint(1, 3),
        'product_price': product_price[product]

    }


def on_send_success(record_metadata):
    print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")


def on_send_error(excp):
    print(f"Issue sending message: {excp}")

Event_list = [item_viewed(), item_added_to_cart(), order_created()]


def run_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers = [kafka_broker],
            value_serializer = lambda x: json.dumps(x).encode('utf-8'),
        )

        print(f"Kafka Producer starting to send events to topic: {kafka_topic} on {kafka_broker}")

    except Exception as e:
        print(f"Error Details : {e}")
        return
    
    try:

        for i in range(0,max_events):
            event = random.choice(Event_list)
            values = event

            sender = producer.send(topic=kafka_topic, value=values)

            sender.add_callback(on_send_success)
            sender.add_errback(on_send_error)

        print(f"\n Successfully sent {max_events} events. Producer concluding normally.")
        producer.flush()


    except Exception as e:
        print(f'Error Message {e}')
        return

    finally:
        producer.close()


if __name__ == '__main__':
    run_producer()