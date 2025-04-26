import requests

url = "https://telematics.oasa.gr/api/?act=getStopArrivals&p1=400075"  # Ένα στάνταρ stopcode (ΗΣΑΠ Ν.Φάληρο)

try:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()
    print("✅ SUCCESS:", data)
except Exception as e:
    print("❌ ERROR:", e)

