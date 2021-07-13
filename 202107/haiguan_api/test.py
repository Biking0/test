from fastapi import FastAPI

app = FastAPI()


@app.get("/{name}")
def read_root(name):
    return {"Hello": "World" + name}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None,start: str=None,end: str=None):
    return {"item_id": item_id, "q": q,"start":start,"end":end}