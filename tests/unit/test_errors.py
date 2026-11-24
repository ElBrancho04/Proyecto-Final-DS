import pickle

from common.errors import PlatformError, Reject


def test_platform_error_survives_pickle():
    # Ray serializa con pickle las excepciones lanzadas dentro de un actor
    err = pickle.loads(pickle.dumps(PlatformError("AUTH_ERROR", "token inválido")))
    assert isinstance(err, PlatformError) and err.code == "AUTH_ERROR" and err.details == "token inválido"


def test_reject_survives_pickle():
    rej = pickle.loads(pickle.dumps(Reject("OVERLOADED", "busy")))
    assert isinstance(rej, Reject) and rej.code == "OVERLOADED" and rej.retryable is True
