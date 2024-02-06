import os
import random
import time
from typing import Dict, List, Optional

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
    free_url: str
    payload: FileAcessRequestPayload


class NewCoordinatorPayload(BaseModel):
    coordinator_name: str


class ElectionData(BaseModel):
    nodes_ids: List[Dict[str, int]]


class NodeMetadata:
    ring: dict
    ring_id: int
    name: str
    is_master: bool
    started_election: bool
    master_node: str
    is_on_election: bool

    def __init__(self) -> None:
        self.ring_id = random.randint(0, 9999)
        self.ring = {
            "node_A": "node_B",
            "node_B": "node_C",
            "node_C": "node_D",
            "node_D": "node_A",
        }
        self.master_node = None
        self.is_on_election = False


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
    node_metadata.is_master = False
    print(f"O nó '{node_metadata.name}' não é o líder.")

# Fila de acessos ao arquivo
acess_queue = []

app = FastAPI()


# Endpoint para verificar se é o nó mestre atual
@app.get("/is_master")
def am_i_master():
    return {"message": node_metadata.is_master}


@app.get("/started_election")
def am_i_started_election():
    return {"message": node_metadata.started_election}


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
        payload = FileAccessFreePayload(
            free_url=f"http://{node_metadata.name}:4000/free_queue", payload=next_access
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
        file.write(
            f"{node_metadata.name} -- {timestamp} -- Acesso permitido por {node_metadata.master_node}\n"
        )

    if payload:
        requests.post(payload.free_url, payload)


@app.post("/new_coordinator_notify")
def updated_coordinator(payload: NewCoordinatorPayload):
    node_metadata.is_on_election = False
    node_metadata.master_node = payload.coordinator_name
    if payload.coordinator_name == node_metadata.name:
        node_metadata.is_master = True
        print(f"O nó {node_metadata.name} agora sabe que é o coordenador!")
    else:
        print(f"O nó {node_metadata.name} sabe quem é o novo coordenador!")


@app.post("/new_coordinator")
def multicast_new_coordinator(coordinator_name: str):
    nodes = list(node_metadata.ring.keys())
    nodes.remove(node_metadata.name)
    nodes.remove(node_metadata.master_node)
    payload = NewCoordinatorPayload(coordinator_name=coordinator_name)

    for node in nodes:
        requests.post(f"http://{node}:4000/new_coordinator_notify", json=payload.dict())

    print(
        f"\nO nó {node_metadata.name} notificou todos na rede a respeito do novo coordenador: {coordinator_name}\n"
    )
    node_metadata.master_node = coordinator_name
    node_metadata.is_on_election = False
    node_metadata.started_election = False


@app.post("/election_process")
def put_id_on_ring(payload: ElectionData):
    # Quando o processo retorna para quem iniciou a eleição
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

    next_payload = ElectionData(
        nodes_ids=payload.nodes_ids.copy()
        + [{node_metadata.name: node_metadata.ring_id}]
    )
    next_node = node_metadata.ring[node_metadata.name]

    try:
        requests.post(
            f"http://{next_node}:4000/election_process", json=next_payload.dict()
        )
    except:
        print(
            f"\nO nó vizinho '{next_node}' está offline! Contactando o próximo nó no anel...\n"
        )
        success_node = node_metadata.ring[next_node]
        print(f"O próximo nó no anel é o {success_node}\n")

        requests.post(
            f"http://{success_node}:4000/election_process", json=next_payload.dict()
        )


def start_election():
    nodes = list(node_metadata.ring.keys())
    nodes.remove(node_metadata.name)
    nodes.remove(node_metadata.master_node)

    for node in nodes:
        try:
            req = requests.get(f"http://{node}:4000/started_election")
            res = req.json()
            if res["message"] is True:  # Verificando se alguém já iniciou a eleição
                print(
                    f"\nO nó {node_metadata.name} identificou que já existe um processo de eleição em andamento!"
                )
                node_metadata.is_on_election = True
                return
        except:
            print(
                f"\nO nó {node_metadata.name} identificou que o nó {node} está offline!"
            )

    print(f"\nO nó {node_metadata.name} é o responsável pela eleição!\n")
    node_metadata.started_election = True
    node_metadata.is_on_election = True
    next_node = node_metadata.ring[node_metadata.name]

    payload = ElectionData(nodes_ids=[{node_metadata.name: node_metadata.ring_id}])
    try:
        requests.post(f"http://{next_node}:4000/election_process", json=payload.dict())
    except:
        print(
            f"\nO nó vizinho '{next_node}' está offline! Contactando o próximo nó no anel...\n"
        )
        success_node = node_metadata.ring[next_node]
        print(f"O próximo nó no anel é o {success_node}\n")

        requests.post(
            f"http://{success_node}:4000/election_process", json=payload.dict()
        )


def request_resource():
    nodes = list(node_metadata.ring.keys())
    nodes.remove(node_metadata.name)

    for node in nodes:
        try:
            req = requests.get(f"http://{node}:4000/is_master")
            res = req.json()
            if res["message"] is True:
                node_metadata.master_node = node
                master_node = node
                break
        except:  # Se a conexão com o nó não pode ser estabelecida
            if node == node_metadata.master_node:  # Se é o nó master, iniciar eleição
                print(
                    f"\nO nó {node} está offline! O nó {node_metadata.name} irá verificar o processo de eleição!"
                )
                start_election()
                return
            else:
                print(
                    f"\nO nó {node} está offline! Porém, não é o nó master! O processamento irá continuar!"
                )

    if master_node != node_metadata.master_node or not node_metadata.master_node:
        node_metadata.is_master = False
        node_metadata.master_node = master_node

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

    # Procurando o nó master inicial na rede
    if not node_metadata.is_master:
        nodes = list(node_metadata.ring.keys())
        nodes.remove(node_metadata.name)

        for node in nodes:
            try:
                req = requests.get(f"http://{node}:4000/is_master")
                res = req.json()
                if res["message"] is True:
                    node_metadata.master_node = node
                    break
            except:
                pass

    # Se não for o master, irá gerar requisições aleatórias
    if not node_metadata.is_master:
        while True:
            if node_metadata.is_on_election:
                continue
            seconds = random.randint(5, 20)
            print(
                f"{node_metadata.name} irá gerar uma solicitação daqui a {seconds} segundos."
            )
            time.sleep(seconds)
            request_resource()
