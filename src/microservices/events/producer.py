import logging

from kafka_config import get_kafka_producer, KAFKA_TOPICS

from models import MovieEvent, UserEvent, PaymentEvent, Event, EventResponse


logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self):
        self.producer = get_kafka_producer()
        self.topics = KAFKA_TOPICS
    
    def publish_movie_event(self, movie_event: MovieEvent) -> EventResponse:
        event = Event(
            type="movie",
            payload=movie_event.dict()
        )
        return self._publish_event(self.topics['movie_events'], event, str(movie_event.movie_id))
    
    def publish_user_event(self, user_event: UserEvent) -> EventResponse:
        event = Event(
            type="user",
            payload=user_event.dict()
        )
        return self._publish_event(self.topics['user_events'], event, str(user_event.user_id))
    
    def publish_payment_event(self, payment_event: PaymentEvent) -> EventResponse:
        event = Event(
            type="payment",
            payload=payment_event.dict()
        )
        return self._publish_event(self.topics['payment_events'], event, str(payment_event.payment_id))
    
    def _publish_event(self, topic: str, event: Event, key: str = None) -> EventResponse:
        try:
            event_dict = event.model_dump(mode='json')

            future = self.producer.send(
                topic=topic,
                key=key,
                value=event_dict
            )
            
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Event published to topic {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
            
            return EventResponse(
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                event_id=event.id
            )
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            raise
    
    def close(self):
        if self.producer:
            self.producer.close()


producer = EventProducer()