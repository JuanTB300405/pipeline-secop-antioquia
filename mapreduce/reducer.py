#!/usr/bin/env python3
# REDUCER: Hadoop entrega las claves ordenadas -> se acumula mientras la clave no cambie
import io, sys
entrada = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
salida = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
actual, conteo, suma = None, 0, 0
for linea in entrada:
    partes = linea.rstrip("\n").split("\t")
    if len(partes) != 3:
        continue
    clave, n, valor = partes[0], int(partes[1]), int(partes[2])
    if clave != actual:
        if actual is not None:
            salida.write("%s\t%d\t%d\n" % (actual, conteo, suma))
        actual, conteo, suma = clave, 0, 0
    conteo += n
    suma += valor
if actual is not None:
    salida.write("%s\t%d\t%d\n" % (actual, conteo, suma))
salida.flush()
