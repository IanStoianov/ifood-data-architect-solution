/*Items por pedido */
CREATE TABLE ifood_trusted.items_por_pedido
WITH (
  format='PARQUET',
  external_location='s3://aws-ifood-ian-triusted/Items/'
) AS
SELECT pedidos.order_id,
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.name') as nomeitem,
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.unitPrice.value') as valorunitario, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.addition.value') as adicionalunitario, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.discount.value') as descontounitario, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.quantity') as quantidade, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.totalValue.value') as valoritem, 
         json_array_length(json_extract(json_array_get(pedidos.items, indices.number-1), '$.garnishItems')) as numeroguarnicoes, 
         json_format(json_extract(json_array_get(pedidos.items, indices.number-1), '$.garnishItems')) as guarnicoes, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.totalAddition.value') as adicionaltotal, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.totalDiscount.value') as descontototal, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.externalId') as externalId, 
         json_extract_scalar(json_array_get(pedidos.items, indices.number-1), '$.integrationId') as integrationId,
         cast(pedidos.items as varchar) items
FROM "ifood_trusted"."pedidos_completo_anonimo" pedidos
CROSS JOIN "ifood_trusted"."indices" 
    WHERE json_array_length(pedidos.items)>= indices.number; 