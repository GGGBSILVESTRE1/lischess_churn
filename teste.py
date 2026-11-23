from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"Hello": "World"}


@app.get("/a")
async def square(num: int):
    result = num**2
    return {"squared": result}
