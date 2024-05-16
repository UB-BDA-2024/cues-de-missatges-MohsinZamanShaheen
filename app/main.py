import fastapi
from .sensors.controller import router as sensorsRouter
from yoyo import read_migrations, get_backend

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/

@app.on_event("startup")
async def apply_timescale_migrations():
    migrations = read_migrations("migrations_ts")
    backend = get_backend('postgresql://timescale:timescale@timescale:5433/timescale')
    backend.apply_migrations(backend.to_apply(migrations))

app.include_router(sensorsRouter)