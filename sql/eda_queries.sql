-- ============================================================
-- 1. RESUMEN GENERAL DEL DATASET
-- ============================================================
SELECT
    COUNT(*)                                    AS total_filas,
    COUNT(DISTINCT DOCUMENTO_NORMALIZADO)       AS tickets_unicos,
    COUNT(DISTINCT ClienteID)                   AS clientes_unicos,
    COUNT(DISTINCT DOCTOR)                      AS doctores,
    MIN(FECHA)                                  AS fecha_min,
    MAX(FECHA)                                  AS fecha_max,
    ROUND(SUM(VENTA_NETA), 2)                   AS venta_neta_total,
    ROUND(AVG(VENTA_NETA), 2)                   AS ticket_promedio
FROM Fact_Lineas_Limpia;

-- ============================================================
-- 2. VENTAS MENSUALES (base para el forecast)
-- ============================================================
SELECT
    FORMAT(FECHA, 'yyyy-MM')                    AS anio_mes,
    COUNT(DISTINCT DOCUMENTO_NORMALIZADO)       AS tickets,
    COUNT(DISTINCT ClienteID)                   AS clientes,
    ROUND(SUM(VENTA_NETA), 2)                   AS venta_neta,
    ROUND(AVG(VENTA_NETA), 2)                   AS ticket_promedio_mes
FROM Fact_Lineas_Limpia
WHERE YEAR(FECHA) IN (2024, 2025)
GROUP BY FORMAT(FECHA, 'yyyy-MM')
ORDER BY anio_mes;

-- ============================================================
-- 3. DETECCIÓN DE DOCUMENTOS SOSPECHOSOS
-- (mismo ticket con muchas líneas puede indicar duplicado)
-- ============================================================
SELECT
    DOCUMENTO_NORMALIZADO,
    FECHA,
    COUNT(*)            AS lineas,
    SUM(VENTA_NETA)     AS total_doc
FROM Fact_Lineas_Limpia
GROUP BY DOCUMENTO_NORMALIZADO, FECHA
HAVING COUNT(*) > 10   -- tickets con más de 10 líneas = revisar
ORDER BY lineas DESC;

-- ============================================================
-- 4. OUTLIERS EN VENTA NETA
-- (valores extremos que pueden ser errores de tipeo)
-- ============================================================
WITH stats AS (
    SELECT
        AVG(VENTA_NETA)                                     AS media,
        STDEV(VENTA_NETA)                                   AS desv,
        PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY VENTA_NETA) OVER ()                   AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY VENTA_NETA) OVER ()                   AS q3
    FROM Fact_Lineas_Limpia
    WHERE VENTA_NETA > 0
)
SELECT
    f.DOCUMENTO_NORMALIZADO,
    f.FECHA,
    f.PRODUCTO,
    f.VENTA_NETA,
    CASE
        WHEN f.VENTA_NETA > s.q3 + 3*(s.q3 - s.q1) THEN 'OUTLIER_ALTO'
        WHEN f.VENTA_NETA < 0                        THEN 'DEVOLUCION'
        ELSE 'OK'
    END AS flag
FROM Fact_Lineas_Limpia f
CROSS JOIN (SELECT TOP 1 * FROM stats) s
WHERE f.VENTA_NETA > s.q3 + 3*(s.q3 - s.q1)
   OR f.VENTA_NETA < 0
ORDER BY f.VENTA_NETA DESC;

-- ============================================================
-- 5. DEVOLUCIONES POR MES
-- ============================================================
SELECT
    FORMAT(FECHA, 'yyyy-MM')    AS anio_mes,
    COUNT(*)                    AS cantidad_devoluciones,
    ROUND(SUM(VENTA_NETA), 2)   AS monto_devuelto,
    COUNT(DISTINCT ClienteID)   AS clientes_afectados
FROM Fact_Lineas_Limpia
WHERE VENTA_NETA < 0
GROUP BY FORMAT(FECHA, 'yyyy-MM')
ORDER BY anio_mes;

-- ============================================================
-- 6. BASE PARA CÁLCULO RFM
-- ============================================================
DECLARE @fecha_ref DATE = (SELECT MAX(FECHA) FROM Fact_Lineas_Limpia);

SELECT
    ClienteID,
    DATEDIFF(DAY, MAX(FECHA), @fecha_ref)           AS recency_dias,
    COUNT(DISTINCT DOCUMENTO_NORMALIZADO)           AS frequency_tickets,
    ROUND(SUM(VENTA_NETA), 2)                       AS monetary_total,
    MIN(FECHA)                                      AS primera_compra,
    MAX(FECHA)                                      AS ultima_compra
FROM Fact_Lineas_Limpia
WHERE VENTA_NETA > 0
  AND ClienteID != 'C00000'      -- excluir clientes anónimos
GROUP BY ClienteID
ORDER BY monetary_total DESC;

-- ============================================================
-- 7. TOP DOCTORES POR VENTA NETA
-- ============================================================
SELECT
    DOCTOR,
    COUNT(DISTINCT DOCUMENTO_NORMALIZADO)       AS tickets,
    COUNT(DISTINCT ClienteID)                   AS clientes_unicos,
    ROUND(SUM(VENTA_NETA), 2)                   AS venta_neta,
    ROUND(SUM(VENTA_NETA) * 1.0 /
        SUM(SUM(VENTA_NETA)) OVER (), 4)        AS pct_venta,
    ROUND(AVG(VENTA_NETA), 2)                   AS ticket_promedio
FROM Fact_Lineas_Limpia
WHERE VENTA_NETA > 0
GROUP BY DOCTOR
ORDER BY venta_neta DESC;

-- ============================================================
-- 8. TOP CATEGORÍAS Y PRODUCTOS
-- ============================================================
SELECT
    CATEGORIA,
    PRODUCTO,
    COUNT(*)                                AS lineas,
    ROUND(SUM(VENTA_NETA), 2)               AS venta_neta,
    ROUND(AVG(VENTA_NETA), 2)               AS precio_promedio,
    ROUND(AVG(DESCUENTO_PCT) * 100, 1)      AS descuento_prom_pct
FROM Fact_Lineas_Limpia
WHERE VENTA_NETA > 0
GROUP BY CATEGORIA, PRODUCTO
ORDER BY venta_neta DESC;

-- ============================================================
-- 9. ANÁLISIS POR SEDE Y CANAL
-- ============================================================
SELECT
    SEDE,
    CANAL,
    COUNT(DISTINCT DOCUMENTO_NORMALIZADO)       AS tickets,
    ROUND(SUM(VENTA_NETA), 2)                   AS venta_neta,
    ROUND(SUM(VENTA_NETA) * 1.0 /
        SUM(SUM(VENTA_NETA)) OVER (), 4)        AS pct_total
FROM Fact_Lineas_Limpia
WHERE VENTA_NETA > 0
GROUP BY SEDE, CANAL
ORDER BY venta_neta DESC;

-- ============================================================
-- 10. CALIDAD: FECHAS Y CAMPOS NULOS O FUERA DE RANGO
-- ============================================================
SELECT
    SUM(CASE WHEN FECHA IS NULL              THEN 1 ELSE 0 END) AS fechas_nulas,
    SUM(CASE WHEN YEAR(FECHA) < 2024         THEN 1 ELSE 0 END) AS antes_2024,
    SUM(CASE WHEN YEAR(FECHA) > 2025         THEN 1 ELSE 0 END) AS despues_2025,
    SUM(CASE WHEN VENTA_NETA IS NULL         THEN 1 ELSE 0 END) AS monto_nulo,
    SUM(CASE WHEN ClienteID IS NULL          THEN 1 ELSE 0 END) AS cliente_nulo,
    SUM(CASE WHEN DOCUMENTO_NORMALIZADO
             NOT LIKE 'B001-%'               THEN 1 ELSE 0 END) AS doc_formato_incorrecto
FROM Fact_Lineas_Limpia;
