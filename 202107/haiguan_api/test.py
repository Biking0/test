from fastapi import FastAPI
import time
import get_data

app = FastAPI()


@app.get("/{name}")
def read_root(name):
    return {"Hello": "World" + name}


@app.get("/get_data/")
def read_item(start: str = None, end: str = None, id: str = None, ):
    # time.sleep(3)
    # time.sleep(3)

    # get_data.get_token()

    return get_data.get_token()
