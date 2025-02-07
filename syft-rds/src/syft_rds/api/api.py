from typing import Any, Union
from pydantic import BaseModel
from syft_rds.connection.connection import RPCConnection

class APIEndpoint(BaseModel):
    endpoint_path: str
    prefix: str
    api_callback: Any
    
    def __call__(self, body: dict | None = None, expiry: str = "5m", cache: bool = True):
        api = self.api_callback()
        res = api.conn.send(
            url=self.endpoint_path,
            body=body,
            expiry=expiry,
            cache=cache,
        )
        return res
    
    def __repr__(self):
        return f"APIEndpoint(prefix={self.prefix}, endpoint_path={self.endpoint_path})"
    def __repr_str__(self):
        return f"APIEndpoint(prefix={self.prefix}, endpoint_path={self.endpoint_path})"
    
def combine_path(prefix: str, path: str) -> str:
    if prefix and path:
        return f"{prefix}/{path}"
    elif prefix:
        return prefix
    else:
        return path

class APIModule(BaseModel):
    relative_module_path: str
    prefix: str
    submodules: dict[str, "APIModule"]
    endpoints: dict[str, APIEndpoint]
    api_callback: Any
    
    @classmethod
    def from_module_list(cls, prefix: str, relative_module_path: str, module_list: list[str], api_callback: Any) -> "APIModule":
        submodules = {}
        endpoints = {}
        
        current_full_path = combine_path(prefix, relative_module_path)
        child_paths = [p for p in module_list if p.startswith(current_full_path) and p != current_full_path]
        for child_path in child_paths:
            child_path_rel_to_current = child_path[len(current_full_path)+1:] # +1 to remove the leading /            
            if "/" in child_path_rel_to_current:
                submodule_name = child_path_rel_to_current.split("/")[0]
                child_relative_module_path = combine_path(relative_module_path, submodule_name)
                submodules[submodule_name] = APIModule.from_module_list(prefix, child_relative_module_path, child_paths, api_callback=api_callback)
            else:
                endpoints[child_path_rel_to_current] = APIEndpoint(endpoint_path=child_path, prefix=prefix, api_callback=api_callback)
        return APIModule(prefix=prefix, relative_module_path=relative_module_path, submodules=submodules, endpoints=endpoints, api_callback=api_callback)

    
    def __getattr__(self, name: str) -> Union["APIModule", "APIEndpoint"]:
        if name in self.submodules:
            return self.submodules[name]
        elif name in self.endpoints:
            return self.endpoints[name]
        else:
            raise AttributeError(f"Module {self.relative_module_path} has no attribute {name}")

class API(BaseModel):
    email: str
    entry_module: APIModule | None = None
    conn: RPCConnection

    def from_email(email: str, conn: RPCConnection) -> "API":
        prefix = f"syft://{email}/api_data/my-rds-app/rpc"
        res = conn.send(
            url=f"{prefix}/apis/list",
            body={},
            expiry="5m",
            cache=True,
        )
        paths = list([str(x) for x in res.keys()])
        _self = API(email=email, entry_module=None, conn=conn)
        def get_api():
            return _self
        
        entry_module = APIModule.from_module_list(prefix, "", paths, api_callback=get_api)
        _self.entry_module = entry_module
        return _self
    
    def __getattr__(self, name: str) -> APIModule | APIEndpoint:
        return getattr(self.entry_module, name)

