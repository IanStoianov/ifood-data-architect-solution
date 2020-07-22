/*Pedidos completo s/ items*/
CREATE TABLE ifood_trusted.pedidos_completo_anonimo
WITH (
  format='PARQUET',
  partitioned_by=array['datapedidolocal'],
  external_location='s3://aws-ifood-ian-triusted/Pedidos/'
) AS
SELECT pedidos.order_id, 
cast(null as varchar) as cpf,
pedidos.customer_id, 
pedidos.delivery_address_city, 
pedidos.delivery_address_country,
pedidos.delivery_address_district,
cast(null as varchar) as delivery_address_external_id,
cast(null as varchar) as delivery_address_latitude,
cast(null as varchar) as delivery_address_longitude,
pedidos.delivery_address_state,
cast(null as varchar) as delivery_address_zip_code,
pedidos.items,
pedidos.merchant_id,
pedidos.merchant_latitude,
pedidos.merchant_longitude,
pedidos.merchant_timezone,
pedidos.horapedido,
pedidos.horapedidoconvertido,
pedidos.order_scheduled,
pedidos.order_total_amount,
pedidos.origin_platform,
pedidos.horastatus,
pedidos.horastatusconvertido,
pedidos.status_id,
pedidos.ultimostatus,
pedidos.language,
pedidos.datacadastrocliente,
pedidos.cadastroativo,
cast(null as varchar) as customer_name,
pedidos.customer_phone_area,
cast(null as bigint) as customer_phone_number,
pedidos.datacadastrorestaurante,
pedidos.restauranteativo,
pedidos.price_range,
pedidos.average_ticket,
pedidos.takeout_time,
pedidos.delivery_time,
pedidos.minimum_order_value,
pedidos.merchant_city,
pedidos.merchant_state,
pedidos.merchant_country, 
cast(pedidos.horapedidoconvertido as date) datapedidolocal/*último campo é usado para particao. Convertido para data de modo a nao gerar um número muito grande de particoes*/
FROM 
    (SELECT pedido.order_id,
         pedido.customer_id,
         pedido.delivery_address_city,
         pedido.delivery_address_country,
         pedido.delivery_address_district,
         pedido.delivery_address_state,
         pedido.items,
         pedido.merchant_id,
         pedido.merchant_latitude,
         pedido.merchant_longitude,
         pedido.merchant_timezone,
         cast(from_iso8601_timestamp(pedido.order_created_at) as timestamp) as horapedido,/*remove o timestamp para armazenamento no S3 de destino*/
         cast(at_timezone(from_iso8601_timestamp(pedido.order_created_at),pedido.merchant_timezone) as timestamp) AS horapedidoconvertido,
         pedido.order_scheduled,
         pedido.order_total_amount,
         pedido.origin_platform,
         cast(from_iso8601_timestamp(status.created_at) as timestamp) AS horastatus,
         cast(at_timezone(from_iso8601_timestamp(status.created_at), pedido.merchant_timezone) as timestamp) AS horastatusconvertido,
         status.status_id,
         status.value AS ultimostatus,/*row_number permite que o status exibido por pedido seja somente o mais recente*/
         consumer.language,
         cast(from_iso8601_timestamp(consumer.created_at) AS date) AS datacadastrocliente,/*convertido para data para não permitir identificacao do cliente*/
         consumer.active AS cadastroativo,
         consumer.customer_phone_area,
         cast(from_iso8601_timestamp(restaurante.created_at) as date) AS datacadastrorestaurante,
         restaurante.enabled restauranteativo,
         restaurante.price_range,
         restaurante.average_ticket,
         restaurante.takeout_time,
         restaurante.delivery_time,
         restaurante.minimum_order_value,
         restaurante.merchant_city,
         restaurante.merchant_state,
         restaurante.merchant_country,
         row_number() over(partition by pedido.order_id
              ORDER BY  from_iso8601_timestamp(pedido.order_created_at) DESC, 
                        from_iso8601_timestamp(status.created_at) desc
                   ) linha
    FROM "demo_db"."ifood_restaurantrestaurant" restaurante /*inicia por tabela menor*/
INNER JOIN "demo_db"."ifood_orderorder" pedido
ON pedido.merchant_id = restaurante.id 
INNER JOIN "demo_db"."ifood_statusstatus" status
        ON pedido.order_id = status.order_id
INNER JOIN "demo_db"."ifood_statusstatus" statusf/*filtra somente os pedidos com equivalencia de data e hora de criação na tabela de status*/
        ON pedido.order_id = statusf.order_id
     AND from_iso8601_timestamp(pedido.order_created_at) = from_iso8601_timestamp(statusf.created_at)
INNER JOIN "demo_db"."ifood_consumerconsumer" consumer
        ON pedido.customer_id = consumer.customer_id
    ) pedidos
WHERE pedidos.linha = 1
;