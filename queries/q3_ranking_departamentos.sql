-- Q3 RANKING: los 10 departamentos más mencionados en las notas
SELECT departamento_mencionado AS departamento,
       COUNT(*) AS notas,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
FROM silver_noticias
WHERE departamento_mencionado IS NOT NULL
GROUP BY departamento_mencionado
ORDER BY ranking
LIMIT 10;
