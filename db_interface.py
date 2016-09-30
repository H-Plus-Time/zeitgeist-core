import zerorpc
import ujson as json

with open("example.json", "r") as f:
    data = json.load(f)

c = zerorpc.Client()
c.connect("tcp://127.0.0.1:4242")
print(c.deposit_article(data))
