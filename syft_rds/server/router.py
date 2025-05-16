from typing import Callable


class RPCRouter:
    def __init__(self):
        self.routes: dict[str, Callable] = {}

    def on_request(self, endpoint: str) -> Callable:
        def register_rpc(func: Callable) -> Callable:
            self.routes[endpoint] = func
            return func

        return register_rpc
