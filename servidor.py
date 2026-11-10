"""
PFO 3 - Programación sobre Redes
Servidor Distribuido con Sockets y Pool de Workers
Alumno: Juan Pablo Pontini
"""

import socket
import threading
import queue
import json
import time
import sys
import logging

# ==========================================
# Configuración de Logging y Constantes
# ==========================================
import os

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "servidor.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

HOST = "localhost"
PUERTO = 6000
NUM_WORKERS = 4          # Cantidad de hilos en el pool de workers
BUFFER_SIZE = 2048
TIMEOUT_CLIENTE = 60.0   # Timeout para conexiones inactivas (mejora Clase 10)

# Cola de tareas donde los clientes depositan solicitudes y los workers las consumen
cola_tareas = queue.Queue()

# Variable de control para apagado ordenado
servidor_activo = True


# ==========================================
# 1. Funciones de Procesamiento de Tareas
# ==========================================
def procesar_tarea_matematica(operacion, a, b=None):
    # Realiza cálculos matemáticos solicitados por el cliente
    if operacion == "suma":
        return a + (b if b is not None else 0)
    elif operacion == "multiplicacion":
        return a * (b if b is not None else 1)
    elif operacion == "potencia":
        return a ** (b if b is not None else 2)
    elif operacion == "factorial":
        import math
        return math.factorial(int(a))
    else:
        return f"Operación matemática desconocida: {operacion}"


def procesar_tarea_texto(operacion, texto):
    # Procesa cadenas de texto enviadas por el cliente
    if operacion == "mayusculas":
        return texto.upper()
    elif operacion == "invertir":
        return texto[::-1]
    elif operacion == "contar_palabras":
        palabras = texto.split()
        return len(palabras)
    elif operacion == "estadisticas":
        return {
            "longitud": len(texto),
            "palabras": len(texto.split()),
            "vocales": sum(1 for c in texto.lower() if c in "aeiouáéíóú")
        }
    else:
        return f"Operación de texto desconocida: {operacion}"


# ==========================================
# 2. Worker / Hilo Trabajador del Pool
# ==========================================
def worker_thread(id_worker):
    # Bucle de ejecución de cada hilo worker del pool
    while servidor_activo:
        try:
            # Espera una tarea de la cola con timeout para permitir cierre limpio
            item = cola_tareas.get(timeout=1.0)
        except queue.Empty:
            continue

        if item is None:
            # Señal de finalización del worker
            break

        tarea, conexion_cliente = item
        id_tarea = tarea.get("id_tarea", "N/A")
        tipo = tarea.get("tipo", "desconocido")
        params = tarea.get("parametros", {})

        logging.info(f"[WORKER {id_worker}] Procesando tarea #{id_tarea} (Tipo: {tipo})")
        tiempo_inicio = time.time()

        # Procesamiento según el tipo de tarea
        resultado = None
        estado = "EXITO"

        try:
            if tipo == "matematica":
                op = params.get("operacion", "suma")
                a = params.get("a", 0)
                b = params.get("b", None)
                resultado = procesar_tarea_matematica(op, a, b)

            elif tipo == "texto":
                op = params.get("operacion", "mayusculas")
                texto = params.get("texto", "")
                resultado = procesar_tarea_texto(op, texto)

            elif tipo == "pesada":
                # Simula procesamiento intensivo
                duracion = params.get("segundos", 1.5)
                time.sleep(duracion)
                resultado = f"Tarea de cómputo distribuido finalizada tras {duracion}s."

            else:
                estado = "ERROR"
                resultado = f"Tipo de tarea no soportado: {tipo}"

        except Exception as e:
            estado = "ERROR"
            resultado = f"Excepción durante el procesamiento: {str(e)}"

        tiempo_total = round(time.time() - tiempo_inicio, 3)

        # Armamos el paquete de respuesta
        respuesta = {
            "id_tarea": id_tarea,
            "estado": estado,
            "resultado": resultado,
            "worker_id": id_worker,
            "tiempo_procesamiento": f"{tiempo_total}s"
        }

        # Enviamos el resultado al cliente por el socket
        try:
            mensaje_json = json.dumps(respuesta) + "\n"
            conexion_cliente.sendall(mensaje_json.encode("utf-8"))
            logging.info(f"[WORKER {id_worker}] Tarea #{id_tarea} completada y enviada ({tiempo_total}s)")
        except Exception as e:
            logging.error(f"[WORKER {id_worker}] Error al responder al cliente para tarea #{id_tarea}: {e}")
        finally:
            cola_tareas.task_done()


# ==========================================
# 3. Inicialización del Pool de Workers
# ==========================================
def inicializar_pool_workers(cantidad=NUM_WORKERS):
    hilos = []
    for i in range(1, cantidad + 1):
        t = threading.Thread(target=worker_thread, args=(i,), daemon=True)
        t.start()
        hilos.append(t)
    logging.info(f"[SISTEMA] Pool de {cantidad} workers inicializado y listo para recibir tareas")
    return hilos


# ==========================================
# 4. Atención de Clientes Conectados
# ==========================================
def atender_cliente(conexion_cliente, direccion_cliente):
    # Recibe una o múltiples tareas de un cliente y las encola
    ip, puerto = direccion_cliente
    logging.info(f"[CONEXIÓN] Cliente conectado desde {ip}:{puerto}")
    conexion_cliente.settimeout(TIMEOUT_CLIENTE)
    buffer = ""

    try:
        while servidor_activo:
            datos = conexion_cliente.recv(BUFFER_SIZE)
            if not datos:
                # El cliente cerró la conexión
                break

            buffer += datos.decode("utf-8")

            # Procesamos mensajes delimitados por salto de línea
            while "\n" in buffer:
                linea, buffer = buffer.split("\n", 1)
                linea = linea.strip()
                if not linea:
                    continue

                try:
                    tarea = json.loads(linea)
                    # Encolamos la tarea junto con la referencia al socket del cliente
                    cola_tareas.put((tarea, conexion_cliente))
                    logging.info(f"[DESPACHADOR] Tarea #{tarea.get('id_tarea')} encolada para distribución a workers")
                except json.JSONDecodeError:
                    error_resp = {
                        "estado": "ERROR",
                        "resultado": "Formato JSON inválido"
                    }
                    conexion_cliente.sendall((json.dumps(error_resp) + "\n").encode("utf-8"))

    except socket.timeout:
        logging.warning(f"[TIMEOUT] Cliente {ip}:{puerto} superó el tiempo de inactividad ({TIMEOUT_CLIENTE}s)")
    except ConnectionResetError:
        logging.warning(f"[AVISO] Cliente {ip}:{puerto} cerró la conexión abruptamente")
    except Exception as e:
        logging.error(f"[ERROR] Problema con cliente {ip}:{puerto}: {e}")
    finally:
        conexion_cliente.close()
        logging.info(f"[DESCONEXIÓN] Conexión cerrada con {ip}:{puerto}")


# ==========================================
# 5. Inicialización del Socket y Bucle Principal
# ==========================================
def inicializar_socket(host=HOST, puerto=PUERTO):
    try:
        servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor_socket.bind((host, puerto))
        servidor_socket.listen(10)
        logging.info(f"[SERVIDOR] Escuchando en {host}:{puerto}...")
        return servidor_socket
    except OSError as e:
        logging.error(f"[ERROR] No se pudo abrir el puerto {puerto}: {e}")
        return None


def main():
    global servidor_activo
    print("=" * 60)
    print("   SERVIDOR DISTRIBUIDO CON POOL DE WORKERS (PFO 3)   ")
    print("=" * 60)

    # 1. Iniciamos el socket
    servidor_socket = inicializar_socket(HOST, PUERTO)
    if not servidor_socket:
        sys.exit(1)

    # 2. Arrancamos el pool de workers
    workers = inicializar_pool_workers(NUM_WORKERS)

    logging.info("[SERVIDOR] Servidor listo. Esperando tareas de clientes... (Ctrl+C para salir)\n")

    # 3. Bucle para aceptar conexiones de clientes entrantes
    try:
        while True:
            conexion, direccion = servidor_socket.accept()
            # Cada conexión de cliente se maneja en un hilo de atención de red
            t_cliente = threading.Thread(target=atender_cliente, args=(conexion, direccion), daemon=True)
            t_cliente.start()

    except KeyboardInterrupt:
        logging.info("\n[SERVIDOR] Deteniendo servidor y workers ordenadamente...")
        servidor_activo = False
    finally:
        servidor_socket.close()
        logging.info("[SERVIDOR] Socket cerrado. Servidor finalizado.")


if __name__ == "__main__":
    main()
