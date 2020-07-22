/*Lista de status por pedido*/
CREATE TABLE ifood_trusted.status_por_pedido
WITH (
  format='PARQUET',
  external_location='s3://aws-ifood-ian-triusted/Status/'
) AS
select pedidos.order_id, 
       pedidos.status_id, 
       pedidos.status,
       pedidos.criacaopedido,
       pedidos.horaevento
from (
SELECT
         pedido.order_id,
         status.status_id,
         status.value status,
         cast(from_iso8601_timestamp(pedido.order_created_at) as timestamp) criacaopedido,
         cast(from_iso8601_timestamp(status.created_at) as timestamp) horaevento, 
        row_number() over(partition by pedido.order_id, status.status_id /*particao no row_number garante pegar só uma entrada por status*/
ORDER BY  from_iso8601_timestamp(pedido.order_created_at) DESC, /*decrescrente para recuperar o valor mais recente por partição*/
          from_iso8601_timestamp(status.created_at) DESC) 
          linha
FROM "demo_db"."ifood_orderorder" pedido
INNER JOIN "demo_db"."ifood_statusstatus" status
    ON pedido.order_id = status.order_id 
  ) pedidos  
  where pedidos.linha = 1;