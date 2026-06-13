import requests
import json

url = "http://localhost:4000/stream/api/retail-transaction"

with requests.get(url, stream=True) as response:
    print("Connected to stream...")

    buffer = ""
    for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
        buffer += chunk
        while "\n\n" in buffer:
            part, buffer = buffer.split("\n\n", 1)
            part = part.strip()
            if not part:
                continue
            try:
                data = json.loads(part)
                print(json.dumps(data, indent=2))
            except json.JSONDecodeError:
                pass
