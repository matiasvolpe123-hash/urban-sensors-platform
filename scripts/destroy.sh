#!/usr/bin/env bash
# Borra toda la plataforma: al eliminar el namespace se elimina todo lo que contiene.
set -euo pipefail

kubectl delete namespace urban-sensors --ignore-not-found
