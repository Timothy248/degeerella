import socket
from time import sleep
from threading import Thread

class Client:
    def __init__(self, address: str, port: int):
        self.sock = None

        self.serverVersion = None
        self.spacer = None
        self.id = None

        self.connected = False
        self.connected = self.connect()
        self.running = False

        self.subbed = []

        self.dataqueue = []
        self.queryqueue = dict()

        self.registered = dict()

    def connect(self):
        if self.connected: return True
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(('localhost', 8232))
            if not self.spacer: self.setup(self.sock.recv(1024))
            self.connected = True
            return True
        except Exception as e: 
            self.connected = False
            print(e)
            return False   

    def setup(self, data: bytes):
        data = data.decode().split(" ")
        _data = dict()
        for i in data:
            i = i.split(":")[:-1]
            _data[i[0]] = i[1]
        self.id = int(_data["id"])
        self.version = _data["version"]
        self.spacer = _data["spacer"]

    def listen(self, callback=None, thread=True, retry=False):
        self.running = self.connect()
        if not self.running:
            if retry: 
                sleep(1)
                self.listen(callback, thread, retry)
            else: return False
        if thread: Thread(target=self._listen, daemon=True, args=(callback, thread, retry)).start()
        else: self._listen(callback, thread, retry)

    def _listen(self, callback, thread, retry):
        try:
            while self.running:
                if not self.connected: sleep(5)
                data = self.sock.recv(1024)
                data = data.decode()
                if callback: callback(data)
                if data.startswith(":") and self.spacer:
                    data = data[1:].split(" ")
                    data[1] = data[1].replace(self.spacer, " ")
                    self.dataqueue.append(data)
                    reg = self.registered.get(data[0])
                    if reg:
                        for i in reg: i(data)
                elif data.startswith("query") and self.spacer:
                    data = data[6:].split(" ")
                    data[1] = data[1].replace(self.spacer, " ")
                    self.queryqueue[data[0]] = data[1]

        except Exception as e:
            print(e)
            self.sock.close()
            self.connected = False
            if retry:
                sleep(1)
                self.listen(callback, thread, retry)
            else: self.running = False

    def listenFor(self, topic, caseId, maxTime=10):
        subbed = self.subscribed(topic)
        if not subbed: self.subscribe(topic)
        found = None
        for _ in range(maxTime):
            for i in self.dataqueue:
                if i[0] == topic and i[1].startswith(str(caseId)):
                    found = i
                    self.dataqueue.remove(i)
                    break
            if found: break
            sleep(1)
        if not subbed: self.subscribe(topic)
        if found: return found[1].split("-")[1:]
    
    def save(self, keyword: str, value): self.send(f"save {keyword} {value.replace(' ', self.spacer)}")

    def get(self, keyword: str, timeout=100):
        if not keyword.strip(): return False
        self.send(f"get {keyword.strip()}")
        counter = 0
        while keyword not in self.queryqueue.keys(): 
            if counter > timeout: return None
            counter += 1
            sleep(0.05)
        data = self.queryqueue[keyword]
        del self.queryqueue[keyword]
        return data if data != "error" else None

    def send(self, data: str):
        if not self.connect(): return False
        self.sock.send(data.encode())
        return True

    def register(self, topic:str, callback):
        if not self.registered.get(topic):
            self.registered[topic] = [callback]
        else: self.registered[topic].append(callback)
    
    def unregister(self, topic:str):
        if self.registered: del self.registered[topic]

    def emit(self, topic:str, data:str):
        self.send(f"emit {topic} {data}")
    
    def subscribed(self, topic): return topic in self.subbed

    def subscribe(self, topic: str, callback=None):
        if not self.subscribed(topic):
            self.send(f"subscribe {topic}")
            self.subbed.append(topic)
            if callback: self.register(topic, callback)

    def unsubscribe(self, topic: str):
        if self.subscribed(topic):
            self.send(f"subscribe {topic}")
            self.subbed.remove(topic)
            self.unregister(topic)

    def disconnect(self): self.running = self.connected = False