import pytest
import ray
from common.messages import create_envelope
from common.config import RAY_NAMESPACE

@pytest.mark.asyncio
async def test_broker_at_least_once_and_dlq():
    # 1. Obtener la referencia al actor BrokerService levantado por Ray en el bootstrap
    try:
        broker = ray.get_actor("broker", namespace=RAY_NAMESPACE)
    except ValueError:
        pytest.skip("El actor 'broker' de Ray no está disponible en este clúster.")

    queue = "cap.math.add"

    # Crear un sobre de prueba
    envelope = create_envelope(
        sender_id="agent-client",
        recipient_id="broker",
        msg_type="request",
        payload={"a": 10, "b": 20, "to_capability": "math.add"}
    )
    message_id = envelope["message_id"]

    # 2. Probar publicación de mensaje
    pub_id = await broker.publish.remote(queue, envelope)
    assert pub_id == message_id, "El ID del mensaje publicado debe coincidir."

    # 3. Probar consumo (debe pasar a estado IN_FLIGHT con intentos = 0)
    msg_consumed = await broker.consume.remote(queue)
    assert msg_consumed is not None, "El broker debería retornar un mensaje pendiente."
    assert msg_consumed["attempts"] == 0, "El número inicial de intentos debe ser 0."
    assert msg_consumed["envelope"]["message_id"] == message_id

    delivery_id = msg_consumed["delivery_id"]

    # Intentar consumir nuevamente de inmediato debe retornar None (está en vuelo / IN_FLIGHT)
    msg_in_flight = await broker.consume.remote(queue)
    assert msg_in_flight is None, "Un mensaje en vuelo (IN_FLIGHT) no debe poder consumirse de nuevo antes de expirar o hacer nack."

    # 4. Simular fallo (nack) - Intento 1 -> Debería volver a PENDING con attempts = 1
    await broker.nack.remote(queue, delivery_id)

    msg_retry_1 = await broker.consume.remote(queue)
    assert msg_retry_1 is not None
    assert msg_retry_1["attempts"] == 1, "El número de intentos debe incrementarse a 1 tras el primer nack."
    del_id_1 = msg_retry_1["delivery_id"]

    # 5. Simular fallo - Intento 2 -> attempts = 2
    await broker.nack.remote(queue, del_id_1)
    msg_retry_2 = await broker.consume.remote(queue)
    assert msg_retry_2 is not None
    assert msg_retry_2["attempts"] == 2
    del_id_2 = msg_retry_2["delivery_id"]

    # 6. Simular fallo - Intento 3 -> attempts = 3 (Al superar los 3 intentos permitidos, va a la DLQ)
    await broker.nack.remote(queue, del_id_2)

    # La cola original ya no debe tener mensajes disponibles
    msg_empty = await broker.consume.remote(queue)
    assert msg_empty is None, "El mensaje debió salir de la cola principal tras agotar los 3 intentos."

    # 7. Verificar estadísticas o la existencia del mensaje en la DLQ (`dlq.math.add`)
    stats = await broker.stats.remote()
    dlq_queue = "dlq.math.add"

    # Comprobamos que la DLQ registra actividad o mensajes en estado DLQ
    assert dlq_queue in stats or any("dlq" in q for q in stats.keys()), "El mensaje debe ser redirigido a la Dead Letter Queue (DLQ)."