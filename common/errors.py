from typing import Optional

from common.messages import DEFAULT_RETRYABLE


class PlatformError(Exception):
    def __init__(self, code: str, details: str):
        self.code = code
        self.details = details
        super().__init__(f"[{code}] {details}")

    def __reduce__(self):
        # Necesario para que Ray pueda deserializar la excepción en el llamador
        return (self.__class__, (self.code, self.details))

class Reject(Exception):
    def __init__(self, code: str, details: str, retryable: Optional[bool] = None):
        if retryable is None:
            retryable = DEFAULT_RETRYABLE.get(getattr(code, "value", code), False)
        self.code = code
        self.details = details
        self.retryable = retryable
        super().__init__(f"Reject [{code}] ({'retryable' if retryable else 'fatal'}): {details}")

    def __reduce__(self):
        return (self.__class__, (self.code, self.details, self.retryable))
