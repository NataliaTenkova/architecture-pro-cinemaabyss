import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, status

from producer import producer
from consumer import consumer
from models import MovieEvent, UserEvent, PaymentEvent, EventResponse
from kafka_config import create_topics_if_not_exist

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info("Starting Events Service...")
    
    # Создаем топики если их нет
    try:
        create_topics_if_not_exist()
    except Exception as e:
        logger.warning(f"Could not create topics: {e}")
    
    # Запускаем консюмеры в фоне
    consumer.start_consuming()
    
    yield
    
    # Остановка
    logger.info("Shutting down Events Service...")
    consumer.stop()
    producer.close()

# Создаем приложение
app = FastAPI(
    title="Events Service",
    description="Kafka-based events service",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/api/events/health")
async def health_check():
    """Проверка работоспособности сервиса"""
    return {
        "status": True,
        "message": "Events Service is healthy"
    }


@app.post("/api/events/movie", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_movie_event(movie_event: MovieEvent, background_tasks: BackgroundTasks):
    """Создание события фильма"""
    try:
        logger.info(f"Received movie event: {movie_event.dict()}")
        
        # Публикуем событие
        response = producer.publish_movie_event(movie_event)
        
        return response
    except Exception as e:
        logger.error(f"Failed to create movie event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/user", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_user_event(user_event: UserEvent, background_tasks: BackgroundTasks):
    """Создание события пользователя"""
    try:
        logger.info(f"Received user event: {user_event.model_dump(mode='json')}")
        
        # Публикуем событие
        response = producer.publish_user_event(user_event)
        
        return response
    except Exception as e:
        logger.error(f"Failed to create user event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_payment_event(payment_event: PaymentEvent, background_tasks: BackgroundTasks):
    """Создание события платежа"""
    try:
        logger.info(f"Received payment event: {payment_event.dict()}")
        
        # Публикуем событие
        response = producer.publish_payment_event(payment_event)
        
        return response
    except Exception as e:
        logger.error(f"Failed to create payment event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/events/status")
async def get_status():
    """Получение статуса сервиса и консюмеров"""
    return {
        "service": "events-service",
        "topics": list(consumer.consumers.keys()),
        "consumers_running": consumer.running,
        "active_threads": len(consumer.threads)
    }

if __name__ == "__main__":
    PORT = int(os.getenv('PORT', '8082'))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info"
    )