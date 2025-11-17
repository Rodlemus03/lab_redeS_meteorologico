import json
import random
import time
import uuid
from confluent_kafka import Producer, Consumer
import matplotlib.pyplot as plt


TOPIC_JSON = "20220000_json"      
TOPIC_COMPACT = "20220000_comp"   
BOOTSTRAP = "147.182.219.133:9092"
WIND_DIRS = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
MAX_MSG = 7



def generar_data():
    temperatura = round(random.gauss(55, 20), 2)
    temperatura = max(0, min(110, temperatura))
    humedad = random.randint(0, 100)
    viento = random.choice(WIND_DIRS)
    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": viento
    }



def producer_normal():
    producer = Producer({'bootstrap.servers': BOOTSTRAP})
    print("\n=== Producer NORMAL (JSON) corriendo (7 mensajes) ===")

    for i in range(MAX_MSG):
        data = generar_data()
        payload = json.dumps(data).encode("utf-8")

        producer.produce(TOPIC_JSON, payload)
        producer.flush()

        print(f"[{i+1}/{MAX_MSG}] Enviado JSON:", data)
        time.sleep(1.0)

    print("✔ Producer NORMAL finalizado.\n")


def consumer_normal():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': f'normal-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC_JSON])

    temps, hums = [], []
    plt.ion()
    fig, ax = plt.subplots(2, 1, figsize=(8, 6))

    print("\n=== Consumer NORMAL (JSON) escuchando (7 mensajes) ===")
    count = 0
    idle_loops = 0

    while count < MAX_MSG:
        msg = consumer.poll(0.5)

        if msg is None:
            idle_loops += 1
            if idle_loops % 10 == 0:
                print("...esperando mensajes JSON...")
            plt.pause(0.05)
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception:
            print("Mensaje ignorado (no es JSON):", msg.value())
            continue

        temps.append(data["temperatura"])
        hums.append(data["humedad"])

        ax[0].clear()
        ax[1].clear()
        ax[0].plot(temps, label="Temperatura °C", marker="o")
        ax[1].plot(hums, label="Humedad %", marker="o")
        ax[0].legend()
        ax[1].legend()
        plt.pause(0.05)

        print(f"[{count+1}/{MAX_MSG}] Recibido JSON:", data)
        count += 1

    plt.ioff()
    plt.savefig("grafica_normal.png")
    print("Imagen guardada como grafica_normal.png\n")
    plt.show()



def encode_payload(temp, hum, wind_index):
    t = int(temp)   
    h = hum         
    w = wind_index  
    packed = (t << 10) | (h << 3) | w
    return packed.to_bytes(3, "big")


def producer_compacto():
    producer = Producer({'bootstrap.servers': BOOTSTRAP})
    print("\n=== Producer COMPACTO (3 bytes) corriendo (7 mensajes) ===")

    for i in range(MAX_MSG):
        temp = random.gauss(55, 20)
        temp = max(0, min(110, temp))
        hum = random.randint(0, 100)
        wind_index = random.randint(0, 7)

        payload = encode_payload(temp, hum, wind_index)
        producer.produce(TOPIC_COMPACT, payload)
        producer.flush()

        print(f"[{i+1}/{MAX_MSG}] Enviado compacto (3 bytes):", payload)
        time.sleep(1.0)

    print("✔ Producer COMPACTO finalizado.\n")



def decode_payload(b):
    packed = int.from_bytes(b, "big")
    t = (packed >> 10) & 0x3FFF
    h = (packed >> 3) & 0x7F
    w = packed & 0x07
    return t, h, WIND_DIRS[w]


def consumer_compacto():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': f'compact-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC_COMPACT])

    temps, hums = [], []
    plt.ion()
    fig, ax = plt.subplots(2, 1, figsize=(8, 6))

    print("\n=== Consumer COMPACTO escuchando (7 mensajes) ===")
    count = 0
    idle_loops = 0

    while count < MAX_MSG:
        msg = consumer.poll(0.5)

        if msg is None:
            idle_loops += 1
            if idle_loops % 10 == 0:
                print("...esperando mensajes compactos...")
            plt.pause(0.05)
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        temp, hum, wind = decode_payload(msg.value())

        temps.append(temp)
        hums.append(hum)

        ax[0].clear()
        ax[1].clear()
        ax[0].plot(temps, label="Temperatura compacta", marker="o")
        ax[1].plot(hums, label="Humedad compacta", marker="o")
        ax[0].legend()
        ax[1].legend()
        plt.pause(0.05)

        print(f"[{count+1}/{MAX_MSG}] Decodificado -> Temp:{temp} Hum:{hum} Viento:{wind}")
        count += 1

    plt.ioff()
    plt.savefig("grafica_compacto.png")
    print("Imagen guardada como grafica_compacto.png\n")
    plt.show()



def menu():
    print("\n====== LABORATORIO 7 - IoT Estación ======")
    print("1) Producer normal (JSON)")
    print("2) Consumer normal (JSON)")
    print("3) Producer compacto (3 bytes)")
    print("4) Consumer compacto (3 bytes)")
    print("5) Salir")

    op = input("\nElegí opción: ")

    if op == "1":
        producer_normal()
    elif op == "2":
        consumer_normal()
    elif op == "3":
        producer_compacto()
    elif op == "4":
        consumer_compacto()
    else:
        print("Saliendo...")


if __name__ == "__main__":
    menu()
