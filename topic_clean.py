from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

def process_subscription(my_project_id, my_subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(my_project_id, my_subscription_id)

    def message_callback(message):
        print(f"Received and discarded message: {message.data.decode('utf-8')}")
        message.ack()

    print(f"Listening for messages on {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=message_callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print("Subscription canceled by user.")
    except TimeoutError:
        print("Timed out waiting for messages.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        streaming_pull_future.cancel()

    print("Finished listening for messages.")

if __name__ == "__main__":
    my_project_id = "your-project-id"
    my_subscription_id = "your-subscription-id"
    
    process_subscription(my_project_id, my_subscription_id)
