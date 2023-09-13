import socket
from threading import Thread
from datetime import datetime
from pymongo import MongoClient

class Server:
    def __init__(self, address: str, port: int, spacer:str=",", log:str=None, mongo:tuple=("", 0)):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen()
        self.stop = False
        
        self.mongo = True
        try: 
            self.db = MongoClient(host=mongo[0], port=mongo[1]).degeerella
            self.db.profiling_info()
        except: self.mongo = False

        self.version = "1.2"

        self._log = log

        self.connections = dict()
        self.cCounter = 0

        self.spacer = spacer.replace(" ", "")

        self.subscriptions = dict()
        self.subscriptionAlert = []
        self.subscriptionRegistered = dict()

        self.log(f"started | mongo: {self.mongo}")
    
    def listen(self, thread=False):
        if thread: Thread(target=self._listen, daemon=True).start()
        else: self._listen()

    def _listen(self):
        while not self.stop:
            try:
                connection, address = self.sock.accept()
                self.cCounter += 1
                self.log(f"id {self.cCounter} connected : {address[0]}")
                self.connections[self.cCounter] = connection
                self.subscriptions[self.cCounter] = []
                Thread(target=self.interact, daemon=True, args=(address, self.cCounter)).start()
            except KeyboardInterrupt: break
    
    def interact(self, address, id):
        connection = self.connections[id]
        self.sendInfos(id)
        try:
            while not self.stop:
                data = connection.recv(1024)
                if not data: break
                self.log(f"received '{data.decode()}' from id {id}")
                if data == b"disconnect": break
                self.process(data.decode(), id)
        except KeyboardInterrupt: pass
        except ConnectionResetError: pass
        except ConnectionAbortedError: pass
        finally: 
            connection.close()
            self.log(f"id {id} disconnected : {address[0]}")
            del self.connections[id]
            del self.subscriptions[id]
            for i in self.subscriptionAlert: i(self.subscriptions)

    def sendInfos(self, id): self.send(id, f"version:{self.version}: spacer:{self.spacer}: id:{id}:")

    def process(self, data: str, id: int):
        command = data.strip().split(" ")[0]
        args = data.strip().split(" ")[1:]

        commands = {
            "subscribe": self.subscribe,
            "emit": self.emit,
            "save": self.save,
            "get": self.get
        }

        if command in commands.keys(): 
            if not commands[command](id, args): self.connections[id].send("error".encode())
        else: self.connections[id].send("unkown".encode())

    def save(self, id, args):
        if not self.mongo: return False
        if len(args) != 2: return False
        keyword, value = args
        if keyword.find(self.spacer) != -1: return False
        value = value.replace(self.spacer, " ")

        result = self.db.storage.find_one({"name": "keywordStorage"})
        if not result: 
            self.db.storage.insert_one({"name": "keywordStorage", "keywords": dict()})
            result = self.db.storage.find_one({"name": "keywordStorage"})

        if value == "$delete": 
            try: del result["keywords"][keyword]
            except: return False
        else: result["keywords"][keyword] = value 

        self.db.storage.update_one({"_id":result["_id"]}, {"$set": {"keywords": result["keywords"]}})
        return True

    def get(self, id, args): 
        if not self.mongo: return False
        if len(args) != 1: return False
        keyword = args[0]
        if keyword.find(self.spacer) != -1: return False

        result = self.db.storage.find_one({"name": "keywordStorage"})
        if not result: 
            self.db.storage.insert_one({"name": "keywordStorage", "keywords": dict()})
            result = self.db.storage.find_one({"name": "keywordStorage"})

        value = None
        try: value = result["keywords"][keyword]
        except: pass

        self.send(id, f"query {keyword} {value.replace(' ', self.spacer) if value else 'error'}")
        return True

    def emit(self, _, args):
        if len(args) < 2: return
        topic = args[0]
        data = self.spacer.join(args[1:])
        sent = False
        for i in self.subscriptions:
            if topic in self.subscriptions[i]:
                self.send(i, f":{topic} {data}")
                sent = True
        return sent

    def subscribe(self, id, args):
        if len(args) != 1: return
        subscriptions: list = self.subscriptions[id]
        if args[0] in subscriptions:
            self.log(f"id {id} unsubbed from '{args[0]}'")
            subscriptions.remove(args[0])
            self.send(id, "0")
        else:
            self.log(f"id {id} subbed to '{args[0]}'")
            subscriptions.append(args[0])
            self.send(id, "1")
        self.subscriptions[id] = subscriptions
        for i in self.subscriptionAlert: i(self.subscriptions)
        return True
    
    def register(self, subscriptionName: str, func):
        reg = self.subscriptionRegistered[subscriptionName]
        if not reg: reg = []
        reg.append(func)

    def log(self, data):
        data = data.replace("\n", " ")
        data = f"{datetime.now().strftime('%a %d.%m. (%H:%M:%S)')} : {data}\n"
        if self._log:
            with open(self._log, "a+") as fp:
                fp.write(data)
        else: print(data[:-1])

    def close(self):
        self.stop = True
        self.sock.close()
        self.log("server closed")

    def events(self, event, func): 
        if event == "subscription": self.subscriptionAlert.append(func)
    def send(self, id: int, data:str): self.connections[id].send(data.encode())