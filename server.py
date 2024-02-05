import os
import random
import time
from typing import Optional

import requests
from fastapi import FastAPI, Response
from fastapi.exceptions import HTTPException
from pydantic import BaseModel


# Payload para solicitar acesso ao recurso compartilhado
class FileAcessRequestPayload(BaseModel):
    node_name: str
    timestamp: float


# Payload para um nó liberar sua posição na fila
class FileAccessFreePayload(BaseModel):
    free_url: str
    payload: FileAcessRequestPayload


# Método para obter o nome do nó
def get_node_name():
    if "node_name" in os.environ:
        node_name = os.environ["node_name"]
        return node_name
    return ""


node_name = get_node_name()

# Verificação se o nó iniciou como o master
if os.environ.get("is_master") == "True":
    is_master = True
    print(f"O nó '{node_name}' é o líder!")

else:
    is_master = False
    print(f"O nó '{node_name}' não é o líder.")

# Fila de acessos ao arquivo
acess_queue = []

app = FastAPI()


# Endpoint para verificar se é o nó mestre atual
@app.get("/is_master")
def am_i_master():
    return {"message": is_master}


# Endpoint para um nó solicitar acesso ao recurso compartilhado
@app.post("/request_acess_to_file")
def request_acess_to_file(payload: FileAcessRequestPayload):
    if not is_master:
        raise HTTPException(400, "Esse nó não é o líder!")

    if len(acess_queue) != 0:
        acess_queue.append(payload)
        return {"message": "Adicionado à fila"}

    if len(acess_queue) == 0:
        acess_queue.append(payload)
        free_queue(payload)
        return {"message": "Acesso permitido"}


# Endpoint para um nó liberar sua posição na fila
@app.post("/free_queue")
def free_queue(payload: FileAcessRequestPayload):
    if not is_master:
        raise HTTPException(400, "Esse nó não é o líder!")

    acess_queue.remove(payload)

    if len(acess_queue) != 0:
        next_access: FileAcessRequestPayload = acess_queue[0]
        payload = FileAccessFreePayload(
            free_url=f"http://{node_name}:4000/free_queue", payload=next_access
        )
        data = payload.json()
        print(f"Permitindo acesso ao nó '{payload.node_name}'")
        requests.post(
            f"http://{next_access.node_name}:4000/access_permitted",
            json=data,
        )

    elif len(acess_queue) == 0:
        print(f"Permitindo acesso ao nó {payload.node_name}")
        requests.post(
            f"http://{payload.node_name}:4000/access_permitted",
            json=None,
        )

    return {"message": "Ação confirmada com sucesso."}


# Endpoint que libera o acesso ao recurso compartilhado para um nó
@app.post("/access_permitted")
def access_permitted(payload: Optional[FileAccessFreePayload] = None):
    with open("./shared_file.txt", "+a") as file:
        timestamp = time.time()
        pid = os.getpid()
        file.write(f"{node_name} -- {timestamp}\n")

    if payload:
        requests.post(payload.free_url, payload)


def request_resource():
    nodes = ["node_A", "node_B", "node_C", "node_D"]
    nodes.remove(node_name)

    for node in nodes:
        req = requests.get(f"http://{node}:4000/is_master")

        if req.status_code != 200:
            # THE NODE IS OFFLINE
            pass
        res = req.json()
        if res["message"] is True:
            master_node = node
            break

    if master_node is None:
        # THE NODE OFFLINE IS THE MASTER
        # START ELECTION
        # GENERATE RANDOM NUMBER AND SEND TO THE OTHER NODES
        pass

    payload = FileAcessRequestPayload(node_name=node_name, timestamp=time.time())
    data = payload.dict()
    requests.post(
        f"http://{master_node}:4000/request_acess_to_file",
        json=data,
    )


if __name__ == "__main__":
    import threading

    import uvicorn

    def start_server():
        uvicorn.run(app, host="0.0.0.0", port=4000)

    # Rodando o servidor HTTP em outra thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    # Se não for o master, irá gerar requisições aleatórias
    if not is_master:
        while True:
            seconds = random.randint(5, 20)
            print(f"{node_name} irá gerar uma solicitação daqui a {seconds} segundos.")
            time.sleep(seconds)
            request_resource()
