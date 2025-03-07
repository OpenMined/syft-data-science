from collections import UserDict, UserList

from syft_rds.jupyter_utils.tabulator import build_tabulator_table


class TableList(UserList):
    def _repr_html_(self) -> str:
        return build_tabulator_table(self.data)


class TableDict(UserDict):
    def _repr_html_(self) -> str:
        return build_tabulator_table(self.data)
