class KeyValueStore:
    def __init__(self):
        self.data = {}

    def put(self, key, value):
        self.data[key] = value
        return True

    def get(self, key):
        value = self.data.get(key, "failed")
        return value, key in self.data

    def doCommit(self, key, value):
        self.data[key] = value
        return True

    def askVote(self, key):
        return self.data.get(key, "failed")

    def discover(self, port):
        print("Discovering new port:", port)
        return list(self.data.keys())

# Crear una instancia de KeyValueStore
store_service = KeyValueStore()
