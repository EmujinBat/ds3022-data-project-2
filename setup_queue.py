import httpx

UVA_ID = "kfm8nx"

def main():
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
    print(f"Seeding queue for UVA_ID='{UVA_ID}' via POST {url} ...")
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(url)
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        raise SystemExit(f"Seeding failed: {e}")

    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        raise SystemExit(f"Unexpected response, no 'sqs_url': {payload}")

    print("Success. Your queue now has 21 delayed messages.")
    print(sqs_url)

if __name__ == "__main__":
    main()