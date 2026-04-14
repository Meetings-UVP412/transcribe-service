def format_time(seconds: float) -> str:
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"({hours:02d}:{minutes:02d}:{secs:02d})"
