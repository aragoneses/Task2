class KeyValueStore:

    def __init__(self):
        self.key_value_pairs = {}

    def put(self, key, value):
        print('Received message with the key: ' + key)
        self.key_value_pairs[key] = value
        return True

    def get(self, key):
        if key in self.key_value_pairs:
            return self.key_value_pairs[key], True
        else:
            return "failed", False

    def doCommit(self, key, value):
        self.key_value_pairs[key] = value
        return True

    def askVote(self, key):
        if key in self.key_value_pairs:
            return self.key_value_pairs[key]
        else:
            return "failed"

    def addSlave(self, port):
        return True


store_service = KeyValueStore()
