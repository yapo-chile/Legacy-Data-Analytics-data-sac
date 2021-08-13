from __future__ import annotations


class BlocketSchemas:

    def __init__(self, params: type[ReadParams]) -> None:
        self.params = params

    def define(self, years_back: int) -> list[str]:
        """
        Define Blocket schemas to use in the queries
        """
        current_year = int(self.params.get_current_year())
        schemas = ['public'] + [f'blocket_{i}' for i in range(current_year - years_back, current_year + 1)]

        return schemas
    