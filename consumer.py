from kafka import KafkaConsumer
from collections import defaultdict
import json



kafka_broker = 'kafka:29092'
kafka_topic = 'retail_events'
group_id = 'revenue_aggregator_group'
TIMEOUT_SECONDS = 10 * 1000  


product_views = defaultdict(int)
product_add_to_cart = defaultdict(int)
product_order_created = defaultdict(lambda: {"count": 0, "total_amount": 0.0})

user_views = defaultdict(int)
user_add_to_cart = defaultdict(int)
user_order_created = defaultdict(lambda: {"count": 0, "total_amount": 0.0})




def product_data():
    print("\n" + "="*80)
    print("PRODUCT AGGREGATION SUMMARY")
    print("="*80)
    print(f"{'Product ID':<13} {'Views':>7} {'Cart Adds':<12} {'Ordered':>10} {'Total Purchase':<15}")
    print("-" * 50)
    product_ids = set(list(product_views.keys()) + list(product_add_to_cart.keys()) + list(product_order_created.keys()))
    for id in sorted(product_ids):
         print(f"{id:<13} {product_views.get(id, 0):>7} {product_add_to_cart.get(id, 0):<12} {product_order_created.get(id, {'count': 0})['count']:>9} {product_order_created.get(id, {'total_amount': 0})['total_amount']:<17}")



def user_data():
    print("\n" + "="*80)
    print("USER AGGREGATION SUMMARY")
    print("="*80)
    print(f"{'User ID':<10} {'Views':>7} {'Cart Adds':<12} {'Ordered':>10} {'Total Purchase':<15}")
    print("-" * 50)
    user_ids = set(list(user_views.keys()) + list(user_add_to_cart.keys()) + list(user_order_created.keys()))
    for id in sorted(user_ids):
         print(f"{id:<13} {user_views.get(id, 0):>7} {user_add_to_cart.get(id, 0):<12} {user_order_created.get(id, {'count': 0})['count']:>9} {user_order_created.get(id, {'total_amount': 0})['total_amount']:<17}")

def run_consumer():

    print(f"--- Consumer initializing. Topic: {kafka_topic}, Broker: {kafka_broker} ---")

    try:

        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=TIMEOUT_SECONDS

        )
    
    except Exception as e:
        print(f"Error {e}")
        return
    


    try:

        for message in consumer:

            event = message.value
            product_id = event.get("product_id") 
            user_id = event.get("user_id")

            
            if event.get('event_type') == 'Item_Viewed':
                    product_views[product_id] +=1
                    user_views[user_id] +=1

            elif event.get('event_type') == 'Added_to_Cart':
                    product_add_to_cart[product_id] += event["quantity"]
                    user_add_to_cart[user_id] +=1

                    product_views[product_id] +=1
                    user_views[user_id] +=1

            elif event.get('event_type') == 'Order_Created':
                    total_amount = event["quantity"] * event["product_price"]

                    product_order_created[product_id]["count"] += event["quantity"]
                    user_order_created[user_id]["count"] +=1

                    product_order_created[product_id]["total_amount"] += total_amount
                    user_order_created[user_id]["total_amount"] += total_amount

                    product_views[product_id] +=1
                    user_views[user_id] +=1

                    product_add_to_cart[product_id] += event["quantity"]
                    user_add_to_cart[user_id] +=1

    except Exception as e:
        print(f"Error: {e}")    
        return       


    finally:
         consumer.close()

if __name__ == '__main__':
    run_consumer()
    product_data()
    user_data()



