#!/usr/bin/env bash
# Despliega toda la plataforma en el cluster de Kubernetes actual.
# Es lo mismo que los comandos "kubectl apply" del README, en un solo script.
set -euo pipefail

cd "$(dirname "$0")/.."
NS=urban-sensors

echo "==> Namespace y ConfigMaps de configuración"
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configmaps.yaml

echo "==> ConfigMaps con el código (productor y job de Spark)"
kubectl create configmap producer-code -n "$NS" --from-file=apps/producer/ \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap spark-job-code -n "$NS" --from-file=apps/spark/ \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Kafka y creación del tópico"
kubectl apply -f k8s/10-kafka.yaml
kubectl rollout status deployment/kafka -n "$NS" --timeout=300s
kubectl apply -f k8s/11-kafka-topic-job.yaml
kubectl wait --for=condition=complete job/kafka-create-topic -n "$NS" --timeout=300s

echo "==> Productor y Spark"
kubectl apply -f k8s/20-producer.yaml
kubectl apply -f k8s/30-spark-streaming.yaml

echo
echo "Listo. Estado de los pods:"
kubectl get pods -n "$NS"
