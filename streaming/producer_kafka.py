#!/usr/bin/env python3
"""
producer_kafka.py — Simule un flux de transactions vers Kafka.

Lit le fichier val.parquet (20% du dataset, 118 108 transactions) depuis GCS
et envoie chaque transaction au topic Kafka 'fraud-transactions' en JSON.

Usage :
  # Depuis la VM (localhost:9094) :
  python3 producer_kafka.py --rate 10

  # Depuis Cloud Shell (IP externe) :
  python3 producer_kafka.py --bootstrap 34.140.133.17:9094 --rate 10

  # Smoke test rapide (200 tx) :
  python3 producer_kafka.py --max 200 --rate 50

Options :
  --rate N       Transactions par seconde (défaut: 10)
  --max N        Nombre max de transactions (défaut: toutes)
  --bootstrap    Adresse Kafka (défaut: localhost:9094)
  --parquet      Chemin GCS du fichier (défaut: gs://pfe-fraud-dataproc-bucket/raw/splits/val.parquet)
"""
import argparse, json, time, sys
import pandas as pd
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser(description="Kafka transaction producer")
    parser.add_argument("--rate", type=int, default=10, help="Tx/sec")
    parser.add_argument("--max", type=int, default=0, help="Max tx (0 = all)")
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap")
    parser.add_argument("--topic", default="fraud-transactions", help="Topic Kafka")
    parser.add_argument("--parquet", default="gs://pfe-fraud-dataproc-bucket/raw/splits/val.parquet",
                        help="GCS path to parquet file")
    args = parser.parse_args()

    print(f"Loading {args.parquet}...")
    df = pd.read_parquet(args.parquet)
    total = len(df) if args.max == 0 else min(args.max, len(df))
    print(f"  {len(df)} rows loaded, will send {total}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )

    interval = 1.0 / args.rate
    sent = 0
    errors = 0
    t0 = time.time()

    try:
        for i, row in df.iterrows():
            if sent >= total:
                break
            try:
                producer.send(args.topic, value=row.to_dict())
                sent += 1
                if sent % 1000 == 0:
                    elapsed = time.time() - t0
                    actual_rate = sent / elapsed
                    print(f"  {sent}/{total} sent ({actual_rate:.1f} tx/s)")
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  ⚠ Send error #{errors}: {e}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n⚠ Interrompu par l'utilisateur")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - t0
        print(f"\n{'='*50}")
        print(f"Terminé : {sent} tx envoyées en {elapsed:.0f}s ({sent/elapsed:.1f} tx/s)")
        print(f"Erreurs : {errors}")

if __name__ == "__main__":
    main()
