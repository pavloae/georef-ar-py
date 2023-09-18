class AddressNormalizationException(Exception):

    def __init__(self, address, response, *args) -> None:
        super().__init__(*args)
        self.address = address
        self.response = response
