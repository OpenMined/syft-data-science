import uvicorn
from syft_rpc import JSONModel
from pathlib import Path
import io
from syft_core import Client
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd

client = Client.load()

app = FastAPI()

current_dir = Path(__file__).parent


# @app.get("/", response_class=HTMLResponse, include_in_schema=False)
# async def chat(request: Request):
#     with open(current_dir / "html" / "page.html") as f:
#         return HTMLResponse(f.read())

users = {"info@openmined.org": "changethis"}


class User(JSONModel):
    email: str
    password: str


@app.post("/login", include_in_schema=False)
async def login(request: Request):
    try:
        body = await request.json()
        user = User(**body)
        print(
            "got body",
            body,
            user.email in users.keys(),
            users[user.email] == user.password,
        )

        if user.email in users.keys() and users[user.email] == user.password:
            return JSONResponse(content={"status": "success"})
        else:
            # wrong password status code 404
            return JSONResponse(
                status_code=404,
                content={"status": "error", "error": "Invalid credentials"},
            )
    except Exception as e:
        print(e)
        return JSONResponse(content={"status": "error", "error": str(e)})


all_tables = ["subreddits"]


all_schemas = ["subreddits"]


@app.get("/tables", include_in_schema=False)
async def tables(request: Request):
    return JSONResponse(content={"tables": list(all_tables)})


@app.get("/tables/{table}", include_in_schema=False)
async def table(request: Request):
    table = request.path_params["table"]
    if table not in all_tables:
        return JSONResponse(
            status_code=404, content={"status": "error", "error": "Table not found"}
        )
    base_df = pd.read_csv("table.csv")
    stream = io.StringIO()
    base_df.to_csv(stream, index=False)
    # base_df.to_csv("table.csv", index=False)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table}.csv"},
    )


@app.get("/schemas", include_in_schema=False)
async def schemas(request: Request):
    return JSONResponse(content={"schemas": list(all_schemas)})


@app.get("/schemas/{schema}", include_in_schema=False)
async def schema(request: Request):
    schema = request.path_params["schema"]
    if schema not in all_schemas:
        return JSONResponse(
            status_code=404, content={"status": "error", "error": "Schema not found"}
        )
    base_df = pd.read_csv("schema.csv")
    # base_df.to_csv("schema.csv", index=False)
    stream = io.StringIO()
    base_df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={schema}.csv"},
    )


def main() -> None:
    debug = True

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=9081,
        log_level="debug" if debug else "info",
        reload=debug,
        reload_dirs="./",
    )


if __name__ == "__main__":
    main()
