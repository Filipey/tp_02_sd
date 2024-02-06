import json
import os
import random
import time
from typing import Any, List, Union

import requests
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from pydantic import BaseModel


# Payload para solicitar acesso ao recurso compartilhado
class FileAcessRequestPayload(BaseModel):
    node_name: str
    timestamp: float


# Payload para um nó liberar sua posição na fila
class FileAccessFreePayload(BaseModel):
    master_node: str
    free_url: str
    metadata: FileAcessRequestPayload


class NewCoordinatorPayload(BaseModel):
    coordinator_name: str


class ElectionData(BaseModel):
    nodes_ids: List[str]


class NodeMetadata:
    ring: dict
    ring_id: int
    name: str
    is_master: bool
    started_election: bool
    master_node: str

    def __init__(self) -> None:
        self.ring_id = random.randint(0, 9999)
        self.ring = {
            "node_A": "node_B",
            "node_B": "node_C",
            "node_C": "node_D",
            "node_D": "node_A",
        }


node_metadata = NodeMetadata()


# Método para obter o nome do nó
def get_node_name():
    if "node_name" in os.environ:
        node_name = os.environ["node_name"]
        return node_name
    return ""


node_metadata.name = get_node_name()

node_metadata.is_master = False
node_metadata.started_election = False

# Verificação se o nó iniciou como o master
if os.environ.get("is_master") == "True":
    node_metadata.is_master = True
    node_metadata.master_node = node_metadata.name
    print(f"O nó '{node_metadata.name}' é o líder!")

else:
    is_master = False
    print(f"O nó '{node_metadata.name}' não é o líder.")

# Fila de acessos ao arquivo
acess_queue = []

app = FastAPI()


# Endpoint para verificar se é o nó mestre atual
@app.get("/is_master")
def am_i_master():
    return {"message": node_metadata.is_master}


# Endpoint para um nó solicitar acesso ao recurso compartilhado
@app.post("/request_acess_to_file")
def request_acess_to_file(payload: FileAcessRequestPayload):
    if not node_metadata.is_master:
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
    if not node_metadata.is_master:
        raise HTTPException(400, "Esse nó não é o líder!")

    print(f"\nFILA: {[item.node_name for item in acess_queue]}\n")

    acess_queue.remove(payload)

    if len(acess_queue) != 0:
        next_access: FileAcessRequestPayload = acess_queue[0]
        metadata = FileAccessFreePayload(
            free_url=f"http://{node_metadata.name}:4000/free_queue",
            metadata=next_access,
            master_node=node_metadata.name,
        )
        print(f"Permitindo acesso ao nó '{payload.node_name}'")
        print(f"Conteúdo enviado: {metadata.json()}")
        requests.post(
            url=f"http://{payload.node_name}:4000/access_permitted",
            json=metadata.json(),
        )

    elif len(acess_queue) == 0:
        next_access = FileAcessRequestPayload(node_name="", timestamp=0.0)
        metadata = FileAccessFreePayload(
            master_node=node_metadata.name, free_url="", metadata=next_access
        )
        print(f"Permitindo acesso ao nó '{payload.node_name}'")
        print(f"Conteúdo enviado: {metadata.json()}")
        requests.post(
            url=f"http://{payload.node_name}:4000/access_permitted",
            json=metadata.json(),
        )


# Endpoint que libera o acesso ao recurso compartilhado para um nó
@app.post("/access_permitted")
def access_permitted(payload: dict):
    with open("./shared_file.txt", "+a") as file:
        timestamp = time.time()
        file.write(
            f"{node_metadata.name} -- {timestamp} -- Acesso concedido pelo master {node_metadata.master_node}\n"
        )

    if payload["free_url"] != "":
        requests.post(url=payload["free_url"], json=payload["metadata"].json())


@app.post("/new_coordinator_notify")
def updated_coordinator(payload: NewCoordinatorPayload):
    if payload.coordinator_name == node_metadata.name:
        node_metadata.is_master = True
        print(f"O nó {node_metadata.name} agora sabe que é o coordenador!")
    else:
        print(f"O nó {node_metadata.name} sabe quem é o novo coordenador!")


@app.post("/new_coordinator")
def multicast_new_coordinator(coordinator_name: str):
    nodes = node_metadata.ring.keys()
    nodes.remove(node_metadata.name)
    payload = NewCoordinatorPayload(coordinator_name=coordinator_name)

    for node in nodes:
        requests.post(f"http://{node}:4000/new_coordinator_notify", json=payload.json())

    print(
        f"\nO nó {node_metadata.name} notificou todos na rede a respeito do novo coordenador: {coordinator_name}\n"
    )


@app.post("/election_process")
def put_id_on_ring(payload: ElectionData):
    if node_metadata.started_election:
        node_metadata.started_election = False
        greather_id = -1

        for node_data in payload.nodes_ids:
            for node, id in node_data.items():
                if id > greather_id:
                    new_master = node
                    greather_id = id

        if new_master == node_metadata.name:
            node_metadata.is_master = True
            node_metadata.master_node = node_metadata.name

        multicast_new_coordinator(new_master)

        return {"message": "Processo finalizado!"}

    current_ids = payload.nodes_ids
    current_ids.append(node_metadata.ring_id)
    next_node = node_metadata.ring[node_metadata.name]
    payload = ElectionData(nodes_ids=current_ids)

    req = requests.post(f"http://{next_node}/election_process", json=payload.json())

    if req.status_code != 200:
        print(
            f"O nó {next_node} também está offline! Contactando o próximo nó no anel..."
        )
        success_node = node_metadata.ring[next_node]
        print(f"O próximo nó no anel é o {success_node}")

        requests.post(f"http://{success_node}/election_process", json=payload.json())


def start_election():
    node_metadata.started_election = True
    next_node = node_metadata.ring[node_metadata.name]

    nodes_ids = [{node_metadata.name: node_metadata.ring_id}]

    payload = ElectionData(nodes_ids=nodes_ids)
    requests.post(f"http://{next_node}:4000/election_process", json=payload.json())


def request_resource():
    nodes = list(node_metadata.ring.keys())
    nodes.remove(node_metadata.name)

    for node in nodes:
        req = requests.get(f"http://{node}:4000/is_master")

        if req.status_code != 200:
            print(f"O nó {node} está offline!")
            break
        res = req.json()
        if res["message"] is True:
            node_metadata.master_node = node
            master_node = node
            break

    if master_node is None:
        start_election()
        return

    payload = FileAcessRequestPayload(
        node_name=node_metadata.name, timestamp=time.time()
    )
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
    if not node_metadata.is_master:
        while True:
            seconds = random.randint(5, 20)
            print(
                f"{node_metadata.name} irá gerar uma solicitação daqui a {seconds} segundos."
            )
            time.sleep(seconds)
            request_resource()
