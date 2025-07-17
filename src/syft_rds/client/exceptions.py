class RDSClientError(Exception):
    pass


class RDSValidationError(RDSClientError):
    pass


class DatasetExistsError(RDSClientError):
    pass


class DatasetNotFoundError(RDSClientError):
    pass


class JobNotFoundError(RDSClientError):
    pass
