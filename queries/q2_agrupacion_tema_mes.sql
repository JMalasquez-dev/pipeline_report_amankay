-- Q2 AGRUPACIÓN: número de notas por mes y tema
SELECT month(fecha) AS mes,
       tema,
       COUNT(*) AS notas
FROM workspace.amankay.gold_noticias
GROUP BY month(fecha), tema
ORDER BY mes, notas DESC;
