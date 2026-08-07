import json
import logging
import threading
from datetime import datetime

from kafka_config import get_kafka_consumer, KAFKA_TOPICS


logger = logging.getLogger(__name__)


class EventConsumer:
    def __init__(self):
        self.consumers = {}
        self.running = False
        self.threads = []
        
    def start_consuming(self):
        """Запускает всех консюмеров в отдельных потоках"""
        self.running = True
        
        for topic in KAFKA_TOPICS.values():
            consumer = get_kafka_consumer(topic, f'events-service-group-{topic}')
            if consumer:
                self.consumers[topic] = consumer
                thread = threading.Thread(target=self._consume_messages, args=(topic, consumer))
                thread.daemon = True
                thread.start()
                self.threads.append(thread)
                logger.info(f"Started consumer for topic: {topic}")
    
    def _consume_messages(self, topic, consumer):
        try:
            for message in consumer:
                if not self.running:
                    break
                
                self._process_message(topic, message)
                
        except Exception as e:
            logger.error(f"Error consuming messages from {topic}: {e}")
    
    def _process_message(self, topic, message):
        """Обрабатывает полученное сообщение"""
        try:
            event = message.value
            event_type = event.get('type', 'unknown')
            payload = event.get('payload', {})
            
            log_message = {
                'timestamp': datetime.now(),
                'topic': topic,
                'event_id': event.get('id'),
                'event_type': event_type,
                'partition': message.partition,
                'offset': message.offset,
                'payload': payload
            }
            
            logger.info(f"Event processed: {json.dumps(log_message, default=str, ensure_ascii=False)}")
            
            if event_type == 'movie':
                self._handle_movie_event(payload)
            elif event_type == 'user':
                self._handle_user_event(payload)
            elif event_type == 'payment':
                self._handle_payment_event(payload)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _handle_movie_event(self, payload):
        action = payload.get('action')
        movie_title = payload.get('title')
        logger.info(f"Movie event: {action} - {movie_title}")
    
    def _handle_user_event(self, payload):
        action = payload.get('action')
        user_id = payload.get('user_id')
        logger.info(f"User event: {action} - user {user_id}")
    
    def _handle_payment_event(self, payload):
        status = payload.get('status')
        amount = payload.get('amount')
        user_id = payload.get('user_id')
        logger.info(f"Payment event: {status} - user {user_id} (${amount})")
    
    def stop(self):
        self.running = False
        for consumer in self.consumers.values():
            consumer.close()
        for thread in self.threads:
            thread.join(timeout=5)
        logger.info("All consumers stopped")


consumer = EventConsumer()