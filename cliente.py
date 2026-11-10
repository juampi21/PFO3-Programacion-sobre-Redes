"""
PFO 3 - Programación sobre Redes
Cliente para Envío de Tareas y Recepción de Resultados
Alumno: Juan Pablo Pontini
"""

import socket
import json
import time
import sys

# Configuración del servidor
HOST = "localhost"
PUERTO = 6000
BUFFER_SIZE = 2048

contador_tareas = 0


# ==========================================
# 1. Conexión con el Servidor
# ==========================================
def conectar_servidor(host=HOST, puerto=PUERTO):
    try:
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((host, puerto))
        return cliente_socket
    except ConnectionRefusedError:
        print(f"\n[ERROR] No se pudo conectar a {host}:{puerto}.")
        print("        Verificá que el servidor esté corriendo con 'python servidor.py'.")
        return None
    except OSError as e:
        print(f"\n[ERROR DE RED] Error al conectar el socket: {e}")
        return None


# ==========================================
# 2. Envío de Tarea y Espera de Resultado
# ==========================================
def enviar_tarea_y_recibir(cliente_socket, tarea):
    try:
        # Codificamos la tarea en JSON y enviamos con delimitador \n
        payload = json.dumps(tarea) + "\n"
        cliente_socket.sendall(payload.encode("utf-8"))

        # Esperamos la respuesta del worker
        respuesta_bytes = cliente_socket.recv(BUFFER_SIZE)
        if not respuesta_bytes:
            print("[AVISO] El servidor cerró la conexión.")
            return None

        linea = respuesta_bytes.decode("utf-8").strip()
        # En caso de que vengan múltiples respuestas en un bloque
        primera_linea = linea.split("\n")[0]
        respuesta = json.loads(primera_linea)
        return respuesta

    except Exception as e:
        print(f"[ERROR] Error durante la comunicación: {e}")
        return None


# ==========================================
# 3. Flujos de Tareas
# ==========================================
def tarea_matematica(cliente_socket):
    global contador_tareas
    contador_tareas += 1

    print("\n--- Tarea Matemática ---")
    print("Operaciones disponibles: suma, multiplicacion, potencia, factorial")
    op = input("Seleccione operación: ").strip().lower()

    if op not in ["suma", "multiplicacion", "potencia", "factorial"]:
        print("Operación no válida.")
        return

    try:
        a = float(input("Ingrese primer número (a): ").strip())
        b = None
        if op != "factorial":
            b = float(input("Ingrese segundo número (b): ").strip())
    except ValueError:
        print("Debe ingresar valores numéricos válidos.")
        return

    tarea = {
        "id_tarea": contador_tareas,
        "tipo": "matematica",
        "parametros": {
            "operacion": op,
            "a": a,
            "b": b
        }
    }

    print(f"\nEnviando tarea #{contador_tareas} al servidor...")
    resp = enviar_tarea_y_recibir(cliente_socket, tarea)
    if resp:
        mostrar_resultado(resp)


def tarea_texto(cliente_socket):
    global contador_tareas
    contador_tareas += 1

    print("\n--- Tarea de Texto ---")
    print("Operaciones disponibles: mayusculas, invertir, contar_palabras, estadisticas")
    op = input("Seleccione operación: ").strip().lower()

    if op not in ["mayusculas", "invertir", "contar_palabras", "estadisticas"]:
        print("Operación no válida.")
        return

    texto = input("Ingrese el texto a procesar: ").strip()
    if not texto:
        print("El texto no puede estar vacío.")
        return

    tarea = {
        "id_tarea": contador_tareas,
        "tipo": "texto",
        "parametros": {
            "operacion": op,
            "texto": texto
        }
    }

    print(f"\nEnviando tarea #{contador_tareas} al servidor...")
    resp = enviar_tarea_y_recibir(cliente_socket, tarea)
    if resp:
        mostrar_resultado(resp)


def enviar_lote_tareas(cliente_socket):
    global contador_tareas
    print("\n--- Demostración de Procesamiento en Paralelo (Lote) ---")
    print("Enviando un lote de 6 tareas para observar cómo el pool de workers las distribuye...\n")

    lote = [
        {"tipo": "matematica", "parametros": {"operacion": "factorial", "a": 12}},
        {"tipo": "texto", "parametros": {"operacion": "invertir", "texto": "Programacion sobre Redes Distribuida"}},
        {"tipo": "pesada", "parametros": {"segundos": 1.0}},
        {"tipo": "matematica", "parametros": {"operacion": "potencia", "a": 2, "b": 16}},
        {"tipo": "texto", "parametros": {"operacion": "estadisticas", "texto": "Sistemas Distribuidos con Sockets y Workers"}},
        {"tipo": "pesada", "parametros": {"segundos": 0.8}}
    ]

    for item in lote:
        contador_tareas += 1
        item["id_tarea"] = contador_tareas
        print(f"Enviando Tarea #{item['id_tarea']} ({item['tipo']})...")
        resp = enviar_tarea_y_recibir(cliente_socket, item)
        if resp:
            mostrar_resultado(resp)
        time.sleep(0.1)


def mostrar_resultado(resp):
    id_t = resp.get("id_tarea")
    estado = resp.get("estado")
    resultado = resp.get("resultado")
    worker = resp.get("worker_id")
    tiempo = resp.get("tiempo_procesamiento")

    print("+" + "-" * 55 + "+")
    print(f"  TAREA #{id_t} - Estado: [{estado}]")
    print(f"  Procesada por: Worker {worker} en {tiempo}")
    print(f"  Resultado: {resultado}")
    print("+" + "-" * 55 + "+\n")


# ==========================================
# 4. Menú Principal
# ==========================================
def menu():
    cliente_socket = conectar_servidor(HOST, PUERTO)
    if not cliente_socket:
        sys.exit(1)

    print("=" * 55)
    print("   SISTEMA DISTRIBUIDO - CLIENTE DE TAREAS (PFO 3)   ")
    print("=" * 55)
    print("[OK] Conectado exitosamente al servidor en localhost:6000\n")

    try:
        while True:
            print("1. Enviar tarea matemática")
            print("2. Enviar tarea de procesamiento de texto")
            print("3. Enviar lote de tareas simultáneas (demo pool)")
            print("4. Salir")

            opcion = input("Seleccione una opción: ").strip()

            if opcion == "1":
                tarea_matematica(cliente_socket)
            elif opcion == "2":
                tarea_texto(cliente_socket)
            elif opcion == "3":
                enviar_lote_tareas(cliente_socket)
            elif opcion == "4":
                print("Cerrando cliente. ¡Hasta luego!")
                break
            else:
                print("Opción no válida. Ingrese un número del 1 al 4.\n")

    except KeyboardInterrupt:
        print("\n[CLIENTE] Sesión interrumpida por el usuario.")
    finally:
        cliente_socket.close()


if __name__ == "__main__":
    menu()
