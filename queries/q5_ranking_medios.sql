-- Q5 RANKING + PARTICIPACIÓN: 10 medios con más notas y su porcentaje acumulado
WITH medios AS (
  SELECT medio, COUNT(*) AS notas
  FROM silver_noticias
  GROUP BY medio
)
SELECT medio,
       notas,
       ROUND(100.0 * notas / SUM(notas) OVER (), 1) AS pct,
       ROUND(100.0 * SUM(notas) OVER (ORDER BY notas DESC, medio ROWS UNBOUNDED PRECEDING) / SUM(notas) OVER (), 1) AS pct_acumulado
FROM medios
ORDER BY notas DESC, medio
LIMIT 10;
