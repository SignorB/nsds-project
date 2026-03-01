# Streamlit + Kafka + Spark – Frontend Template

## Setup

```bash
# 1. Crea e attiva un virtual environment (opzionale ma consigliato)
python -m venv venv
source venv/bin/activate   # macOS/Linux

# 2. Installa le dipendenze
pip install -r requirements.txt

# 3. Avvia l'app
streamlit run app.py
```

## Requisiti di sistema

| Servizio | Versione minima |
|----------|-----------------|
| Python   | 3.9+            |
| Java     | 11+ (richiesto da Spark) |
| Kafka    | 3.x             |

## Avviare Kafka localmente (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  apache/kafka:latest
```

## Struttura

```
frontend/
├── app.py            # Applicazione principale
├── requirements.txt  # Dipendenze Python
└── README.md
```

