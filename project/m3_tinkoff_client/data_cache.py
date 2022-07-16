from tinkoff.invest import Instrument


class DataCache:
    def __init__(self) -> None:
        self._data: dict[str, Instrument] = {}

    def get(self, key: str) -> Instrument:
        return self._data.get(key)

    def put(self, key: str, value: Instrument) -> None:
        self._data[key] = value
        self._update(key, value)

    def put_without_update(self, key: str, value: Instrument) -> None:
        self._data[key] = value

    def _update(self, key: str, value: Instrument) -> None:
        pass
