from importlib import resources
from typing import IO, Literal, LiteralString


class ResourceLoader:
    def load_module_file(
        self,
        module: str,
        filename: str,
        encoding="utf-8",
        mode: Literal["r", "rb"] = "rb",
    ) -> IO:
        """
        Open a resource file bundled within a Python package.

        Handles both text ('r') and binary ('rb') modes safely.
        Automatically omits encoding when opening in binary mode.
        """
        file_path = resources.files(module).joinpath(filename)

        # encoding is only valid for text mode
        if mode == "rb":
            return file_path.open(mode)
        return file_path.open(mode, encoding=encoding)

    def load_sql_file(self, filename: str) -> LiteralString:
        """
        Load the contents of a SQL file
        """
        with self.load_module_file(
            module="OceanDB.sql", filename=filename, mode="r", encoding="utf-8"
        ) as f:
            query = f.read()
            return query
