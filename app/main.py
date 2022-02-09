from fastapi import FastAPI
from dapr.ext.fastapi import DaprApp
f

app = FastAPI()
dapr_app = DaprApp(app)


@dapr_app.subscribe(pubsub='pubsub', topic='some_topic')
def event_handler(event_data):
    print(event_data)