class PlatformError(Exception):
    def __init__(self, code: str, details: str):
        self.code = code
        self.details = details
        super().__init__(f"[{code}] {details}")

class Reject(Exception):
    def __init__(self, code: str, details: str, retryable: bool = True):
        self.code = code
        self.details = details
        self.retryable = retryable
        super().__init__(f"Reject [{code}] ({'retryable' if retryable else 'fatal'}): {details}")