import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

resp = client.responses.create(
    model=model,
    input=[{
        "role": "user",
        "content": [{"type": "input_text", "text": "Say hello in one short line"}]
    }]
)

# Preferred: use the convenience property
text = getattr(resp, "output_text", None)

# Fallback: scan blocks if needed (older/newer shapes)
if not text:
    parts = []
    for block in getattr(resp, "output", []) or []:
        if getattr(block, "type", "") == "output_text":
            parts.append(block.text)
    text = "\n".join(parts) if parts else "(no text found)"

print("Response:", text)
