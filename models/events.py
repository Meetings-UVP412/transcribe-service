class ChunkDownloadedEvent:
    def __init__(self, uuid_str: str, ord_num: int, is_last: bool, duration: int):
        self.uuid = uuid_str
        self.ord = ord_num
        self.isLast = is_last
        self.duration = duration

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            uuid_str=str(data.get('uuid') or data.get('UUID') or data.get('Uuid') or ''),
            ord_num=int(data.get('ord') or data.get('Ord') or data.get('ORD') or 0),
            is_last=bool(data.get('isLast') or data.get('islast') or data.get('IsLast') or False),
            duration=int(data.get('duration') or data.get('Duration') or data.get('DURATION') or 0)
        )

    def __str__(self):
        return f"ChunkDownloadedEvent(uuid={self.uuid}, ord={self.ord}, isLast={self.isLast}, duration={self.duration})"
