import json
import uuid
from typing import Dict, Any, Callable
from dataclasses import dataclass, asdict
from confluent_kafka import Producer, Consumer
import redis
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Abstract Factory Pattern for Event Creation
class EventFactory:
    @staticmethod
    def create_event(event_type: str, data: Dict[str, Any]) -> 'Event':
        event_classes = {
            'user_registration': UserRegistrationEvent,
            'order_created': OrderCreatedEvent,
            # Có thể mở rộng thêm các loại event khác
        }
        
        event_class = event_classes.get(event_type)
        if not event_class:
            raise ValueError(f"Unsupported event type: {event_type}")
        
        return event_class(**data)

# Base Event Class - Template Method Pattern
@dataclass
class Event:
    event_id: str = None
    timestamp: float = None
    
    def __post_init__(self):
        if not self.event_id:
            self.event_id = str(uuid.uuid4())
        if not self.timestamp:
            import time
            self.timestamp = time.time()
    
    def validate(self):
        """Template method for validation"""
        raise NotImplementedError("Subclasses must implement validation")
    
    def to_dict(self):
        return asdict(self)

# Concrete Event Classes
@dataclass
class UserRegistrationEvent(Event):
    username: str
    email: str
    
    def validate(self):
        if not self.username or not self.email:
            raise ValueError("Username and email are required")

@dataclass 
class OrderCreatedEvent(Event):
    user_id: str
    product_id: str
    quantity: int
    
    def validate(self):
        if self.quantity <= 0:
            raise ValueError("Quantity must be positive")

# Observer Pattern - Event Handler
class EventHandler:
    def __init__(self, kafka_bootstrap_servers: str, redis_host: str = 'localhost', redis_port: int = 6379):
        # Kafka Producer Configuration
        self.producer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'event-producer'
        }
        
        # Kafka Consumer Configuration
        self.consumer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'event-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        
        # Redis Client
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)
        
        # Kafka Producer and Consumer
        self.producer = Producer(self.producer_config)
        self.consumer = Consumer(self.consumer_config)
        
        # Event Handlers Registry - Strategy Pattern
        self.event_handlers = {}
    
    def register_handler(self, event_type: str, handler: Callable):
        """Register event handlers dynamically"""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
    
    def publish_event(self, event: Event, topic: str):
        """Publish event to Kafka"""
        try:
            event.validate()  # Validate before publishing
            serialized_event = json.dumps(event.to_dict()).encode('utf-8')
            
            # Caching event in Redis
            self.redis_client.setex(
                f"event:{event.event_id}", 
                3600,  # 1 hour expiry 
                serialized_event
            )
            
            # Publish to Kafka
            self.producer.produce(topic, serialized_event)
            self.producer.flush()
            
            logger.info(f"Event {event.event_id} published to {topic}")
        except Exception as e:
            logger.error(f"Error publishing event: {e}")
    
    def consume_events(self, topics: list):
        """Consume events from Kafka and route to handlers"""
        self.consumer.subscribe(topics)
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    event_data = json.loads(msg.value().decode('utf-8'))
                    event_type = event_data.get('__class__')
                    
                    # Routing Event - Dispatch Pattern
                    if event_type in self.event_handlers:
                        for handler in self.event_handlers[event_type]:
                            handler(event_data)
                    else:
                        logger.warning(f"No handler for event type: {event_type}")
                
                except json.JSONDecodeError:
                    logger.error("Invalid JSON event")
                except Exception as e:
                    logger.error(f"Event processing error: {e}")
        
        except KeyboardInterrupt:
            logger.info("Stopping event consumption")
        finally:
            self.consumer.close()

# Example Event Handlers - Strategy Pattern
def user_registration_handler(event_data):
    logger.info(f"Processing User Registration: {event_data}")
    # Ví dụ: Gửi email, tạo profile, v.v.

def order_creation_handler(event_data):
    logger.info(f"Processing Order Creation: {event_data}")
    # Ví dụ: Kiểm tra kho, xử lý thanh toán, v.v.

def main():
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    
    # Khởi tạo Event Handler
    event_handler = EventHandler(KAFKA_BOOTSTRAP_SERVERS)
    
    # Đăng ký các event handlers
    event_handler.register_handler('UserRegistrationEvent', user_registration_handler)
    event_handler.register_handler('OrderCreatedEvent', order_creation_handler)
    
    # Tạo và publish events
    user_event = EventFactory.create_event('user_registration', {
        'username': 'john_doe',
        'email': 'john@example.com'
    })
    event_handler.publish_event(user_event, 'user_events')
    
    order_event = EventFactory.create_event('order_created', {
        'user_id': 'user123',
        'product_id': 'prod456', 
        'quantity': 2
    })
    event_handler.publish_event(order_event, 'order_events')
    
    # Bắt đầu consume events
    event_handler.consume_events(['user_events', 'order_events'])

if __name__ == '__main__':
    main()