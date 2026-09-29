class PatchError(Exception):
    """Applying a patch was aborted, the feed was not changed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class PatchLoadError(PatchError):
    """The patch file could not be read or is invalid."""

    def __init__(self, path: str, message: str, code: str = "schema_invalid") -> None:
        super().__init__(code, f"{path}: {message}" if path else message)
        self.path = path
