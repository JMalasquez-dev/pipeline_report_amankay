-- Q1 FILTRO: últimas 10 notas de sismo que mencionan a Lima
SELECT fecha, medio, titulo
FROM workspace.amankay.gold_noticias
WHERE tema = 'sismo'
  AND departamento_mencionado = 'LIMA'
ORDER BY fecha DESC
LIMIT 10;
