class AtlasError(Exception):
    """Base class for all AtlasQ exceptions."""


class AtlasIndexFieldError(AtlasError):
    pass


class AtlasIndexError(AtlasError):
    pass


class AtlasFieldError(AtlasError):
    pass
