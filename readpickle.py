import pickle
import json
file = open('connections.pkl', 'rb')
conn = pickle.load(file)

print(json.dumps(conn, indent=4))