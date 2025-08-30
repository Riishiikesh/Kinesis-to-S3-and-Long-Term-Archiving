import boto3
import json
import time
import random
from datetime import datetime
from faker import Faker

# Initialize clients
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')
fake = Faker()

def generate_event():
    """Generate a sample website event"""
    event_types = ['click', 'page_view', 'purchase', 'signup', 'logout']
    
    return {
        'event_id': fake.uuid4(),
        'timestamp': datetime.utcnow().isoformat(),
        'event_type': random.choice(event_types),
        'user_id': fake.uuid4(),
        'session_id': fake.uuid4(),
        'page_url': fake.url(),
        'user_agent': fake.user_agent(),
        'ip_address': fake.ipv4(),
        'country': fake.country_code(),
        'device_type': random.choice(['desktop', 'mobile', 'tablet']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        'referrer': fake.url() if random.random() > 0.3 else None,
        'value': round(random.uniform(0, 1000), 2) if random.random() > 0.7 else None
    }

def send_to_kinesis(event, stream_name='website-events-stream'):
    """Send event to Kinesis stream"""
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(event),
            PartitionKey=event['user_id']
        )
        return response
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")
        return None

def main():
    """Main producer loop"""
    print("Starting data producer...")
    print("Press Ctrl+C to stop")
    
    record_count = 0
    
    try:
        while True:
            
            event = generate_event()
            response = send_to_kinesis(event)
            
            if response:
                record_count += 1
                print(f"Sent record {record_count}: {event['event_type']} - {event['timestamp']} - {response['ShardId']}")
            
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print(f"\nStopping producer. Total records sent: {record_count}")

if __name__ == "__main__":
    main()