# ToDo

## Interacción 1

- sanitycheck.

### Sanity Checks
- validar que el host es valido.
- validar que la conexion es valida.
- Validar que type: AggregateFunction(argMax, Float32, DateTime) este entre comillas "AggregateFunction(argMax, Float32, DateTime)"

## Interacción 2

- Acction create-mv
- Acction drop-mv
- Acction create-table
- Acction drop-table
- Acction truncate
- Acction populate

### Popular with chunks

Para calcular los chunks, ver la particion de la tabla y obtener todas opciones, si es por fecha, todas las fechas.
Ese valor hay que usarlo o incluirlo en el where / prewere.

key visit_at, key
poner un order by por key ORDER BY (visit_at, key)

WHERE visit_at = '2025-07-01'  AND event IN ('enriched_visit_acknowledged', 'enriched_comparison_visited')
  AND site_id != ''
GROUP BY visit_at, key ORDER BY (visit_at, key);

## Interacción 3

- Testing.
- Imprimir en una línea el progress.

## Interacción 4

- Permirir multiple archivos. path=referencia al directorio donde estan los manifiestos
- cada archivo indica dentro del yaml el orden a ejecutar.

## Interacción 5

- Permitir valores por defecto:
  engine: MergeTree
  ttl: visit_at + toIntervalDay(30)
  settings:
    - index_granularity = 8192
    - ttl_only_drop_parts = 1
    - enable_mixed_granularity_parts = 1
