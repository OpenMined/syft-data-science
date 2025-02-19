import functools


class RPCRouter:
    def __init__(self):
        self.routes: dict[str, callable] = {}

    def on_request(self, endpoint: str) -> callable:
        @functools.wraps
        def register_rpc(func):
            self.routes[endpoint] = func
            return func

        return register_rpc
