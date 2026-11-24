# Guion de sustentación (10 min + 5 de preguntas) · 8 diapositivas

Reparto sugerido: **Persona A** presenta las diapositivas 1 a 4 y **Persona B** las 5 a 8. Las preguntas pueden ir a cualquiera, así que los dos estudian todo el documento.

| # | Diapositiva | Contenido | Quién | Tiempo |
|---|---|---|---|---|
| 1 | Pregunta y fuente | Pregunta, usuarios (veedurías, periodistas, Contraloría), SECOP II por API, filtro Antioquia, nº de registros, `id_registro = id_contrato`, `fecha_de_firma` | A | 1 min |
| 2 | Arquitectura Lambda | Diagrama raw → curada → servicio con la herramienta de cada etapa; por qué Lambda y no Kappa | A | 1,5 min |
| 3 | Ingesta y Kafka | Sobre JSON, paginación, reintentos, manifiesto, marca de agua; tópico de 3 particiones, key = id_registro, cómo evitamos duplicados | A | 1,5 min |
| 4 | HDFS y MapReduce | Rutas `fecha_ingesta=…`, un archivo por lote (`hdfs dfs -ls`), tabla MapReduce vs Spark: coinciden | A | 1 min |
| 5 | Zona curada | Tabla de pasos con filas antes y después, decisiones clave, partición `anio_mes`, tamaño JSONL vs Parquet | B | 1,5 min |
| 6 | Calidad e idempotencia | 7 validaciones (5 críticas), reporte con descartados, `curar(completo=True, simular_fallo=True)`, **demo en vivo: 2 corridas con los mismos conteos** | B | 1,5 min |
| 7 | Streaming | Ventana de 1 día, watermark de 3 días, checkpoint en HDFS, reinicio con 0 mensajes; unión con el batch | B | 1 min |
| 8 | Tablero y respuesta | Título que responde la pregunta, 2 o 3 hallazgos, fuente y fecha de corte | B | 1 min |

**Demo en vivo** (diapositiva 6). **Abran Colab y den `Ejecutar todas` unos 25 minutos antes de su turno**, porque el entorno no se guarda entre sesiones. En la sustentación, agreguen una celda al final y ejecuten:
```python
print(pipeline_completo("en_vivo", segundos_streaming=30))
display(pd.read_csv(EVID / "idempotencia_conteos.csv"))
```
Hay que señalar que `curada_filas` y `curada_ids_distintos` son iguales a los de la corrida anterior (o que solo crecieron por contratos realmente nuevos).

---

## Preguntas probables y cómo responderlas

**¿Por qué `id_contrato` es un buen id_registro?**
Es el identificador que SECOP II asigna a cada contrato electrónico (CO1.PCCNTR.…) y no se repite. `proceso_de_compra` no sirve, porque un proceso puede generar varios contratos.

**¿Qué pasa si corren el productor dos veces?**
Kafka no deduplica: los mensajes quedarían dos veces en el tópico, en la misma partición porque tienen la misma key. Lo evitamos con el registro de archivos publicados. Si igual se cuela un duplicado, la zona curada lo elimina con el dedup por id, y la evidencia es que `curada_filas == ids distintos`.

**¿Por qué la key = id_registro?**
Porque el mismo contrato siempre va a la misma partición, y eso mantiene el orden de sus versiones. Además, la carga se reparte entre las 3 particiones.

**¿Por qué un archivo por lote y no por mensaje?**
HDFS guarda los metadatos de cada archivo en la memoria del NameNode. Millones de archivos pequeños la saturan, y además cada archivo ocupa un bloque lógico. Lo mejor son pocos archivos grandes.

**¿Por qué usan `>=` en la marca de agua y no `>`?**
Con `>` perderíamos contratos firmados en el mismo milisegundo que la marca. Con `>=` traemos un traslape mínimo, que el dedup elimina.

**¿Por qué no imputan 0 a los valores nulos?**
Porque bajaría artificialmente el valor promedio. El contrato se cuenta, pero no entra en las sumas ni en los promedios.

**¿Por qué particionan por `anio_mes`?**
Las consultas y el tablero son mensuales, y la carga incremental solo reescribe los meses afectados. Por `ciudad` quedarían 125 particiones muy desiguales (Medellín concentra la mayoría), y por día cientos de archivos pequeños.

**¿Qué es el watermark?**
Es el límite de cuánto retraso aceptamos en tiempo de evento. Con 3 días, Spark cierra la ventana de un día cuando ya vio eventos 3 días más recientes, la emite y libera su estado. Lo que llegue más tarde se descarta.

**¿Qué guarda el checkpoint?**
Los offsets de Kafka ya procesados y el estado de las ventanas abiertas. Al reiniciar continúa desde ahí, y la evidencia es la corrida de reinicio con 0 mensajes leídos y los mismos conteos.

**¿Lambda o Kappa, y por qué?**
Lambda. El histórico necesita limpieza completa y validación contra lo ya curado, y eso se hace en batch. La capa rápida da los conteos diarios recientes, y la tabla `ind_serie_diaria` une las dos.

**¿Qué pasa si falla una validación crítica?**
Se lanza `CalidadCriticaError`, el pipeline se detiene y la zona curada no se toca (el conteo es el mismo antes y después). Se demuestra con `curar(completo=True, simular_fallo=True)`.

**¿Por qué MapReduce y Spark dan lo mismo?**
Porque aplican exactamente las mismas reglas sobre los mismos archivos raw: modalidad vacía → SIN_DATO, valor no numérico → 0 y redondeo HALF_UP. Si no coincidieran, la columna `coincide` de la comparación lo mostraría.

**¿Por qué Parquet ocupa menos?**
Es columnar: guarda juntos los valores de cada columna, que se comprimen muy bien, y no repite los nombres de los campos en cada registro como hace el JSON. Además permite leer solo las columnas y las particiones necesarias.

**¿Por qué la limpieza no descartó ninguna fila en la primera corrida?**
En este rango de fechas la API entregó registros completos: todos tenían id, fecha y valor válidos. Igual la limpieza sí actuó en dos casos:
- La normalización convirtió en nulo el municipio "No Definido" de 3.162 contratos; por eso aparecen como "Sin dato" en el tablero.
- En la corrida 2, el dedup eliminó el registro repetido que trajo el traslape de la marca de agua (4.022 → 4.021 en el mes afectado).

Las reglas quedan ahí para proteger las cargas futuras, y la demo de `simular_fallo` muestra que la validación sí detiene el pipeline.

**¿Por qué el streaming cuenta 55.406 contratos y el batch 55.963?**
Por el watermark de 3 días. Las ventanas de los últimos días (19 al 22 de noviembre) siguen abiertas en el estado del checkpoint, esperando eventos más recientes, y no se emiten hasta que el watermark pase su final. Las 557 diferencias están en esas ventanas. El batch sí tiene el dato completo, y por eso la serie diaria usa el batch hasta su fecha de corte (Lambda).

**¿Por qué la corrida 2 trajo 1 registro?**
Porque la marca de agua usa `>=`: vuelve a pedir el contrato firmado justo en la última fecha cargada. Pasó por Kafka y HDFS, y la zona curada lo deduplicó: 55.963 filas = 55.963 ids.

**¿Qué explica el salto de julio?**
La Ley de Garantías prohibió la contratación directa del 31 de enero al 21 de junio de 2026. Entre marzo y mayo solo el 10–14 % de los contratos fue directo. En julio, ya sin la restricción, se firmaron 15.768 contratos y el 91 % fue directo.
