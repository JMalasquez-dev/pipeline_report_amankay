-- Q4 COMPARATIVA: notas por tema, primer vs segundo semestre
WITH base AS (
  SELECT tema,
         SUM(CASE WHEN month(fecha) <= 6 THEN 1 ELSE 0 END) AS semestre_1,
         SUM(CASE WHEN month(fecha) >  6 THEN 1 ELSE 0 END) AS semestre_2
  FROM workspace.amankay.gold_noticias
  GROUP BY tema
)
SELECT tema, semestre_1, semestre_2,
       ROUND(100.0 * (semestre_2 - semestre_1) / NULLIF(semestre_1, 0), 1) AS variacion_pct
FROM base
ORDER BY variacion_pct DESC;
