#!/usr/bin/env python3
# MAPPER: sobre JSON -> modalidad \t 1 \t valor_entero
import io, json, sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
entrada = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
salida = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

def a_entero(valor):
    if valor is None:
        return 0
    try:
        return int(Decimal(str(valor).strip()).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return 0

for linea in entrada:
    linea = linea.strip()
    if not linea:
        continue
    try:
        sobre = json.loads(linea)
    except ValueError:
        salida.write("__CORRUPTO__\t1\t0\n")
        continue
    p = sobre.get("payload") or {}
    modalidad = (p.get("modalidad_de_contratacion") or "").strip().replace("\t", " ") or "SIN_DATO"
    salida.write("%s\t1\t%d\n" % (modalidad, a_entero(p.get("valor_del_contrato"))))
salida.flush()
