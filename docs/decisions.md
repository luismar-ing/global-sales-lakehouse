# Registro de decisiones — Global Sales Lakehouse

Cada entrada registra qué se decidió, qué alternativa se descartó y por qué. Las entradas están agrupadas por capa y en el orden en que se tomaron.

---

## Infraestructura

### D01 — Compute Serverless en Databricks Premium
- **Alternativa:** compute híbrido (clusters en una VNet propia).
- **Razón:** el foco del proyecto es el pipeline de datos, no el networking. Premium es necesario para los secret scopes respaldados por Key Vault.

### D02 — Unity Catalog con Access Connector para leer ADLS
- **Alternativa:** configurar el Service Principal con `spark.conf.set` en cada notebook.
- **Razón:** en Serverless con Unity Catalog, el acceso a almacenamiento externo pasa por Storage Credential + External Location. El Access Connector usa una identidad administrada, sin secretos que rotar.

### D03 — Dos identidades separadas
- **Decisión:** el Service Principal `sp-gsl-airflow` autentica los scripts locales que suben a `landing`; el Access Connector autentica a Databricks.
- **Razón:** son consumidores distintos del mismo almacenamiento. Unity Catalog nunca usa el Service Principal.

### D04 — Roles de mínimo privilegio
- **Decisión:** `Storage Blob Data Contributor` sobre la cuenta `adlsgsl` y `Key Vault Secrets User` sobre `kv-projectGSL`, no roles a nivel suscripción.
- **Razón:** el acceso a datos requiere roles del plano de datos; `Owner` a nivel suscripción no los otorga y daría permisos de más.

### D05 — Una External Location por contenedor
- **Alternativa:** una sola location en la raíz de la cuenta.
- **Razón:** permite controlar el acceso por capa (`landing`, `bronze`, `silver`, `gold`).

### D06 — File Events omitidos (Force Create)
- **Alternativa:** otorgar al Access Connector los roles de Event Grid y Storage Queue.
- **Razón:** con pocos archivos cargados una sola vez, el listado de directorio no tiene costo relevante. Se reconsidera si la ingesta pasa a ser continua.

### D07 — Git folder solo de lectura en Databricks
- **Decisión:** el código se edita en el IDE local y en Databricks solo se hace pull, con el repo público sin token.
- **Razón:** el repositorio es la única fuente de verdad y no hay que vincular credenciales de GitHub.

---

## Landing

### D08 — Hash SHA-256 en un archivo `.sha256` junto a cada archivo
- **Alternativa:** un único `manifest.json`.
- **Razón:** el número de fuentes es fijo y pequeño; un archivo por fuente es más simple de leer desde el notebook.

### D09 — Frankfurter como un solo backfill histórico
- **Alternativa:** una llamada diaria particionada por fecha.
- **Razón:** las ventas son históricas y necesitan la tasa del día de cada venta. Una llamada al endpoint de series de tiempo trae todo el rango (2009-12-01 → 2014-12-31, unión de ambos datasets).

### D10 — USD como moneda base
- **Razón:** estándar para reportes internacionales; Superstore ya está en USD.

### D11 — REST Countries v5 con paginación y validación de contenido
- **Contexto:** v3.1 respondió `200 OK` con un mensaje de deprecación en el cuerpo.
- **Decisión:** migrar a v5, paginar guiado por `meta.more`, verificar contra `meta.total`, limitar las iteraciones y validar la estructura de cada respuesta antes de subirla.
- **Razón:** `raise_for_status()` no detecta errores que llegan con código 200.

### D12 — API key de REST Countries en `.env`, no en Key Vault
- **Alternativa:** guardarla en Key Vault como el secreto del Service Principal.
- **Razón:** tiene un solo consumidor (un script local) y los datos, una vez en Bronze, no la vuelven a necesitar. Se reconsidera si la ingesta corre en un worker remoto.

---

## Bronze

### D13 — Tablas Delta externas
- **Alternativa:** tablas administradas.
- **Razón:** conservar la separación física por capa en contenedores propios. Implica ejecutar `OPTIMIZE` manualmente.

### D14 — Columnas de auditoría por fila
- **Decisión:** `_ingestion_timestamp`, `_source_file` y `_source_hash`.
- **Razón:** el time travel de Delta versiona la tabla completa y se purga con `VACUUM`; las columnas dan linaje permanente por fila.

### D15 — Idempotencia por nombre de archivo + hash
- **Alternativa:** solo por nombre de archivo.
- **Razón:** un archivo corregido con el mismo nombre debe cargarse; uno idéntico no. La verificación se hace contra toda la tabla Bronze, que es append-only.

### D16 — Idempotencia manual antes que Auto Loader
- **Razón:** entender el problema antes de usar la herramienta. Auto Loader con checkpoints es el refactor natural.

### D17 — Escritura en modo `append`
- **Alternativa:** `overwrite`.
- **Razón:** Bronze conserva el historial de cargas; `overwrite` solo aplica en `landing`.

### D18 — Fechas de Superstore como texto en Bronze
- **Contexto:** leídas como `DATE`, las 51,290 fechas en formato `dd-MM-yyyy` quedaron en `NULL` sin error.
- **Alternativa:** declarar el formato en la lectura.
- **Razón:** Bronze preserva el dato crudo; si el formato falla, el valor original sigue disponible. Se convierten en Silver.

### D19 — Lectura de CSV con `escape='"'` y `FAILFAST`
- **Contexto:** comillas escapadas como `""` en nombres de producto desalinearon 300 filas; `Profit` y `Shipping_Cost` recibieron valores de otras columnas sin generar nulos.
- **Razón:** `escape` corrige el caso; `FAILFAST` convierte cualquier fallo de lectura futuro en un error visible en lugar de un dato incorrecto.

### D20 — Bronze no se modifica salvo datos que nunca fueron datos
- **Decisión:** se borró y reingirió la tabla de REST Countries que solo contenía el mensaje de deprecación.
- **Razón:** una respuesta de error no es una versión legítima de los datos.

---

## Silver

### D21 — Clasificación de transacciones en Online Retail
- **Alternativa:** filtrar y descartar filas atípicas.
- **Decisión:** columna `transaction_type` (`sale`, `cancellation`, `fee_or_charge`, `fee_reversal`, `internal_adjustment`, `gift_voucher`, `manual_adjustment`, `unclassified`, `invalid`).
- **Razón:** cada caso queda visible y explicado; solo `invalid` (errores confirmados y datos de prueba) se excluye.

### D22 — `unclassified` en lugar de `invalid` para filas sin descripción ni precio
- **Razón:** no hay evidencia de que sean errores; la etiqueta comunica que no se pudo determinar.

### D23 — Deduplicación según la clave natural de cada fuente
- **Decisión:** Frankfurter por `(rate_date, currency)`; Superstore por `Row_ID`; REST Countries por `uuid`; Online Retail por la combinación completa de columnas de negocio.
- **Razón:** `_source_hash` es constante dentro de un archivo; particionar por él colapsa toda la tabla en una fila.

### D24 — Placeholder `GUEST` para ventas sin cliente
- **Razón:** son ventas legítimas de compradores sin cuenta.

### D25 — `Postal_Code` excluido de Superstore
- **Razón:** el análisis es a nivel país; 80.5% de nulos. Bronze conserva la columna.

### D26 — Sin filtro `Ship_Date >= Order_Date`
- **Razón:** el EDA no encontró casos; la verificación en Silver lo confirma.

### D27 — `Market_Region` en Superstore
- **Contexto:** `Region` sola es ambigua (`North` existe en `EU` y `LATAM`).
- **Razón:** una clave de agrupación sin ambigüedad, conservando `Region` original.

### D28 — Continentes y monedas de cada país como texto con todos sus valores
- **Alternativa:** tomar el primer elemento de cada arreglo.
- **Razón:** el orden de los arreglos es alfabético y no indica el valor principal; tomar el primero sería arbitrario.

### D29 — `region` como dimensión geográfica, sin tabla puente
- **Alternativa:** tabla puente país-continente.
- **Razón:** los totales siguen siendo aditivos; con la tabla puente, los países transcontinentales se contarían en dos continentes.

### D30 — Tabla de alias de países
- **Decisión:** `silver.countries_alias` traduce nombres de las ventas a los de REST Countries.
- **Razón:** la corrección vive en una tabla versionada, no repartida entre consultas.

### D31 — Cuatro países sin mapear
- **Decisión:** `Channel Islands`, `Unspecified`, `European Community` y `West Indies` quedan como `Unmapped`.
- **Razón:** ninguno corresponde a un país único; inventar una correspondencia asignaría códigos falsos.

### D32 — `Korea` → `South Korea`
- **Razón:** asignación plausible; el dataset no distingue entre las dos Coreas. Documentada como suposición.

---

## Gold

### D33 — Una sola tabla de hechos para ventas y cancelaciones
- **Alternativa:** una tabla por tipo de transacción.
- **Razón:** mismo grano y mismas dimensiones; los importes con signo dan ventas netas con un solo `SUM`.

### D34 — Grano de `fact_sales`: una fila por línea de transacción
- **Razón:** es el nivel más fino disponible en ambas fuentes.

### D35 — Conversión con la tasa del día de cada venta
- **Alternativa:** una tasa única o la tasa actual.
- **Razón:** convertir ventas históricas con una tasa de otra fecha distorsiona las métricas.

### D36 — Forward fill de tasas en `daily_rates`
- **Contexto:** Frankfurter no publica tasas en fines de semana ni feriados.
- **Razón:** un `JOIN` exacto dejaría esas ventas sin tasa, sin error. `fill_type` marca cada tasa como `actual` o `forward`.

### D37 — Backward fill disponible pero no usado en GBP
- **Razón:** usar una tasa posterior a la venta introduce sesgo de información futura. Se marca como `backward` para que sea visible; GBP no lo necesita.

### D38 — Claves sustitutas con `xxhash64`
- **Alternativa:** `ROW_NUMBER()`.
- **Razón:** el hash da siempre la misma clave al reconstruir la tabla; `ROW_NUMBER` puede reasignarlas.

### D39 — Claves de cliente y producto incluyen el sistema de origen
- **Razón:** un mismo ID puede existir en ambas fuentes y representar entidades distintas.

### D40 — Clave natural de país basada en `uuid`
- **Alternativa:** `country_name`.
- **Razón:** los nombres cambian (Swaziland → Eswatini); el `uuid` no.

### D41 — Miembro desconocido `-1` en cada dimensión
- **Razón:** las ventas sin entidad resuelta no se pierden en un `INNER JOIN` y aparecen como un grupo propio.

### D42 — `NULL` en las medidas que una fuente no tiene
- **Alternativa:** `0`.
- **Razón:** `profit`, `discount` y `shipping_cost` no existen en Online Retail; un `0` afirmaría un valor y sesgaría los promedios.

### D43 — `Customer_ID` de Online Retail sin sufijo `.0`
- **Razón:** que el ID sea el que un usuario reconocería.

### D44 — Bandera `is_guest`
- **Razón:** `GUEST` agrupa a muchos compradores y distorsiona las métricas por cliente.

### D45 — Bandera `is_zero_price`
- **Alternativa:** excluir esas ventas.
- **Razón:** no alteran los ingresos pero sí las unidades y el precio promedio; la bandera permite filtrarlas sin perder el registro.

### D46 — Productos de Online Retail: SCD Tipo 1 con la descripción más frecuente
- **Alternativa:** SCD Tipo 2, o `(código, descripción)` como clave.
- **Razón:** las diferencias son ruido de captura. El desempate por descripción hace la elección determinista. Limitación: algunos códigos podrían agrupar productos distintos.

### D47 — Productos de Superstore: clave `(Product_ID, Product_Name)`
- **Contexto:** 457 IDs compartidos por productos distintos (ej. `Motorola Office Telephone` y `Motorola Moto X`).
- **Razón:** con un solo nombre por ID, las ventas de un producto se asignarían a otro.

### D48 — Códigos de producto normalizados con `UPPER(TRIM())`
- **Razón:** 170 códigos aparecen con variantes de mayúsculas para el mismo producto.

### D49 — `fx_rate` como unidades de moneda local por 1 USD
- **Razón:** una sola fórmula para ambas monedas: `amount_usd = amount_local / fx_rate`.

### D50 — Liquid clustering por `date_key`
- **Alternativa:** `PARTITIONED BY (date_key)`.
- **Razón:** con ~1 millón de filas, particionar por día crearía 1,857 particiones muy pequeñas.

---

## Pendientes de registrar

Agregar aquí las decisiones de orquestación, Auto Loader y dbt conforme se tomen.