Archivos de datos:  
* orders.csv (pedidos/ventas)
* customers.csv (clientes)
* products.csv (productos)

Objetivo:  
Convertir estos datos “crudos” en una base confiable para reportes, y presentar un dashboard de BI listo para revisión.

Entregables (obligatorios):
1. Dashboard BI
    * Para Power BI: un archivo .pbix (ej. roseamor_dashboard.pbix)
2. Consultas SQL y snapshot de la Db:
   * Archivos .sql
3. README detallado  
Se quiere un README bien explicado (tipo documentación técnica), incluyendo al menos:
    * Arquitectura/flujo (ej. raw → staging → consumo)
    * Limpieza de datos: qué problemas encontraste (duplicados, nulos, fechas inválidas, negativos, etc.), qué hiciste con cada uno y por qué
    * Modelo de datos: cómo organizaste tablas/dimensiones/hechos y relaciones
    * Cómo ejecutar: pasos claros para correr tu solución en otra computadora
    * Cómo actualizar: qué hacer si mañana llega un CSV nuevo (proceso de refresh)
4. App web (simple) para registrar pedidos:  
Crear una pantalla web mínima para registrar un pedido con estos campos:
order_id, customer_id, sku, quantity, unit_price, order_date, channel.  
    Requisitos mínimos:
    * Guardar el registro en una base de datos (PostgreSQL)
    * Validaciones básicas (campos obligatorios, no negativos, formato de fecha)
    * Explicar en el README cómo correr la app y dónde queda la data

Requisitos del Dashboard (Power BI):  
* KPIs (tarjetas):
  * Ventas totales
  * Margen total (o utilidad)
  * Número de pedidos
  * Ticket promedio (ventas / pedidos)
* Visualizaciones:
  * Ventas por mes
  * Ventas por canal
  * Margen por categoría
  * Top 10 clientes por ingresos
  * Top 10 productos más vendidos
* Filtros:
  * Rango de fecha
  * Canal
  * Categoría
  * País (si aplica)
