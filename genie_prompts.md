# Prompts para Genie Agents (tabla `workspace.amankay.silver_noticias`)

Crea el Genie space con la tabla `silver_noticias` y escribe cada prompt en una **nueva conversación**.
Toma una captura del prompt, del SQL que genera Genie y del resultado. Copia el SQL generado en la carpeta `queries/`
(si Genie genera algo distinto del archivo incluido, conserva el de Genie y ajusta el informe).

## Consultas (SQL o Python)
1. Muéstrame las 10 notas más recientes sobre sismos que mencionan a Lima, con fecha, medio y título.
2. ¿Cuántas notas hay por mes y por tema? Ordénalas por mes y de mayor a menor.
3. Dame un ranking de los 10 departamentos más mencionados en las notas, ignorando los nulos.
4. Compara el número de notas por tema entre el primer y el segundo semestre y calcula la variación porcentual.
5. ¿Qué 10 medios publican más notas y qué porcentaje acumulado representan?
6. (Python) ¿Cuál es el tema dominante de cada mes? Hazlo con PySpark.

## Predicciones a futuro (no son consultas; describe la respuesta y pega capturas)
P1. Con base en el historial de 2025, ¿qué meses de 2026 tendrán más notas sobre emergencias y cuántas se esperarían?
P2. ¿Qué departamento será el más mencionado en las notas de 2026 y qué tan probable es que se mantenga en el primer lugar?
P3. ¿Qué riesgo de aumento de notas sobre sismos existe en 2026 y en qué periodos podría darse?
