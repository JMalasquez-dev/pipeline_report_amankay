# Genie Agents · Noticias Gold de Amankay

Añade `workspace.amankay.gold_noticias` al agente. En una conversación, pide primero el rango de fechas y el total de notas. Después envía los tres prompts de proyección en orden y conserva el SQL y el resultado de cada uno.

## Tres proyecciones

1. **Volumen de cobertura.** Usa los últimos tres meses calendario completos antes del mes de la fecha máxima de la tabla. Calcula el promedio mensual de notas y úsalo como estimación para el primer mes calendario posterior a la fecha más reciente entre hoy y la fecha máxima de la tabla. Muestra los meses de referencia, sus conteos, el mes proyectado y el supuesto de estabilidad. Indica cuántos meses separan el final del historial del mes objetivo.
2. **Departamento con mayor cobertura.** Usando los mismos tres meses, cuenta las notas por departamento mencionado, excluye los nulos y proyecta el ranking de los tres primeros para el mes objetivo según su promedio mensual. Muestra el promedio de cada uno y la diferencia entre los dos primeros.
3. **Cambio en la cobertura de sismos.** Para los mismos tres meses, muestra el conteo mensual de notas con `tema = 'sismo'`. Compara el último mes completo con el promedio del período y estima el conteo del mes objetivo usando ese promedio. Explica la limitación del historial y que el indicador mide cobertura periodística de sismos.

Para cada respuesta, pide una tabla base con los conteos, el SQL y una explicación del método. Incluye meses con cero registros en los conteos de la serie cuando el período esté cubierto por los datos. Si la cobertura histórica es insuficiente, pide que identifique el dato que falta y registra esa limitación. Estas estimaciones son escenarios basados en el histórico disponible.

## Consultas de comprobación

1. Muéstrame las 10 notas más recientes sobre sismos que mencionan a Lima, con fecha, medio y título.
2. ¿Cuántas notas hay por mes y por tema? Ordénalas por mes y de mayor a menor.
3. Dame los 10 departamentos más mencionados, ignorando los nulos.
4. Compara el número de notas por tema entre el primer y el segundo semestre y calcula la variación porcentual.
5. ¿Qué 10 medios publican más notas y qué porcentaje acumulado representan?
