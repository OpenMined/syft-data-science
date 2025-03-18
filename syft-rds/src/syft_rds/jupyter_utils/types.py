from collections import UserDict, UserList
import os
from syft_rds.jupyter_utils.tabulator import build_tabulator_table

SYFT_NO_REPR_HTML = "SYFT_NO_REPR_HTML" in os.environ


class TableList(UserList):
    def _repr_html_(self) -> str:
        if SYFT_NO_REPR_HTML:
            return None
        return build_tabulator_table(self.data)


class TableDict(UserDict):
    def _repr_html_(self) -> str:
        if SYFT_NO_REPR_HTML:
            return None
        return build_tabulator_table(self.data)
