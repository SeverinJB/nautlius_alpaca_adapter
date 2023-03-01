class AlpacaError(Exception):
    """
    The base class for all 'Alpaca' specific errors.
    """

    def __init__(self, status, message, headers):
        self.status = status
        self.message = message
        self.headers = headers


class AlpacaServerError(AlpacaError):
    """
    Represents an 'Alpaca' specific 500 series HTTP error.
    """

    def __init__(self, status, message, headers):
        super().__init__(status, message, headers)


class AlpacaClientError(AlpacaError):
    """
    Represents an 'Alpaca' specific 400 series HTTP error.
    """

    def __init__(self, status, message, headers):
        super().__init__(status, message, headers)
