from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.infra.db import engine, init_sqlite_pragmas
from app.infra.models import Base
from app.web.api import router
from app.web.export import router as export_router
from app.web.auth import (
    admin_router,
    initialize_auth,
    router as auth_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Сначала настраиваем SQLite, затем проверяем/создаём схему.
    init_sqlite_pragmas()
    Base.metadata.create_all(bind=engine)

    # Авторизация живёт только в web-слое. Инициализируем её после
    # создания схемы БД; производственная state-machine здесь не затрагивается.
    initialize_auth()
    yield


app = FastAPI(
    title="ABCD Web",
    lifespan=lifespan,
)
app.include_router(router, prefix="/api")
app.include_router(auth_router, prefix="/api")
app.include_router(admin_router, prefix="/api")

app.include_router(export_router, prefix="/api")
