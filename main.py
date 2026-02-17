import whisper


def format_time(seconds):
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"({hours:02d}:{minutes:02d}:{secs:02d})"


model = whisper.load_model("small")

result = model.transcribe(
    "audio",
    language="ru",
    fp16=False,
    beam_size=5,
    best_of=5,
    temperature=0.0
)

for segment in result["segments"]:
    start_formatted = format_time(segment["start"])
    end_formatted = format_time(segment["end"])
    print(f"{start_formatted} — {segment['text']}")
