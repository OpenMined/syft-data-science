class RDSClientError(Exception):
    pass


class RDSValidationError(RDSClientError):
    pass
