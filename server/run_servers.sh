#!/bin/bash

# Puertos a utilizar
ports=(9042 9044 9046 9048 9050)

# Ejecutar cada servidor en una terminal separada
for port in "${ports[@]}"; do
    gnome-terminal -- bash -c "cargo run -- -p $port; exec bash"
done