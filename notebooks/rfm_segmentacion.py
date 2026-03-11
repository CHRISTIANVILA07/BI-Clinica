"""
=============================================================
  RFM SEGMENTACIÓN DE CLIENTES — BI DERMATOLÓGICO
=============================================================
  Input : Fact_Lineas_Limpia.csv
  Output: Dim_Cliente_RFM.csv

  Columnas output:
    ClienteID, Recency, Frequency, Monetary,
    R_Score, F_Score, M_Score, RFM_Score, Segmento

  Segmentos (basado en RFM Score combinado):
    Champions, Loyal, Potential Loyalist, New Customer,
    Promising, Need Attention, At Risk,
    Cant Lose Them, Hibernating, Lost
=============================================================
"""

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
# 1. CARGAR DATOS
# ─────────────────────────────────────────────
df = pd.read_csv(
    "C:/Users/HP/Downloads/Fact_Lineas_Limpia.csv",
    encoding="utf-8-sig"
)

print(f"Registros cargados: {len(df):,}")
print(f"Columnas: {df.columns.tolist()}")

df.columns = df.columns.str.strip()

# ─────────────────────────────────────────────
# 2. LIMPIEZA
# ─────────────────────────────────────────────
df["FECHA"] = pd.to_datetime(df["FECHA"], dayfirst=True, errors="coerce")
df["VENTA NETA"] = pd.to_numeric(df["VENTA NETA"], errors="coerce")

df = df.dropna(subset=["ClienteID", "FECHA"])
df = df[df["VENTA NETA"] > 0]

print(f"Registros válidos para análisis: {len(df):,}")

# ─────────────────────────────────────────────
# 3. FECHA DE REFERENCIA
# ─────────────────────────────────────────────
fecha_ref = df["FECHA"].max() + pd.Timedelta(days=1)
print(f"Fecha de referencia: {fecha_ref.date()}")

# ─────────────────────────────────────────────
# 4. CALCULAR RFM
# ─────────────────────────────────────────────
rfm = df.groupby("ClienteID").agg(
    Recency=("FECHA", lambda x: (fecha_ref - x.max()).days),
    Frequency=("DOCUMENTO_NORMALIZADO", "nunique"),
    Monetary=("VENTA NETA", "sum")
).reset_index()

print(f"\nClientes únicos: {len(rfm):,}")
print(rfm[["Recency", "Frequency", "Monetary"]].describe().round(2))

# ─────────────────────────────────────────────
# 5. SCORING RFM
# ─────────────────────────────────────────────
def rfm_score(series, reverse=False):

    series = series.rank(method="first")

    try:
        score = pd.qcut(series, 5, labels=[1,2,3,4,5])
    except:
        score = pd.cut(series, 5, labels=[1,2,3,4,5])

    score = score.astype(float).fillna(1)

    if reverse:
        score = 6 - score.astype(int)

    return score.astype(int)

rfm["R_Score"] = rfm_score(rfm["Recency"], reverse=True)
rfm["F_Score"] = rfm_score(rfm["Frequency"])
rfm["M_Score"] = rfm_score(rfm["Monetary"])

rfm["RFM_Score"] = (
    rfm["R_Score"].astype(str) +
    rfm["F_Score"].astype(str) +
    rfm["M_Score"].astype(str)
)

rfm["RFM_Total"] = (
    rfm["R_Score"] * 0.4 +
    rfm["F_Score"] * 0.35 +
    rfm["M_Score"] * 0.25
)

# ─────────────────────────────────────────────
# 6. SEGMENTACIÓN
# ─────────────────────────────────────────────
def segmentar(row):

    r = row["R_Score"]
    f = row["F_Score"]

    if r >= 4 and f >= 4:
        return "Champions"

    elif r >= 3 and f >= 3:
        return "Loyal Customers"

    elif r >= 4 and f <= 2:
        return "New Customers"

    elif r == 3 and f <= 2:
        return "Promising"

    elif r == 2 and f >= 3:
        return "At Risk"

    elif r <= 2 and f >= 4:
        return "Cant Lose Them"

    elif r <= 2 and f <= 2:
        return "Hibernating"

    elif r >= 3:
        return "Loyal Customers"

    else:
        return "Lost"

rfm["Segmento"] = rfm.apply(segmentar, axis=1)

# ─────────────────────────────────────────────
# 7. RESUMEN DE SEGMENTOS
# ─────────────────────────────────────────────
resumen = rfm.groupby("Segmento").agg(
    Clientes=("ClienteID", "count"),
    Venta_Prom=("Monetary", "mean"),
    Recency_Prom=("Recency", "mean"),
    Frequency_Prom=("Frequency", "mean")
).sort_values("Clientes", ascending=False).round(2)

print("\n=== DISTRIBUCIÓN DE SEGMENTOS ===")
print(resumen)

champion_share = (
    rfm[rfm["Segmento"] == "Champions"]["Monetary"].sum() /
    rfm["Monetary"].sum()
)

print(f"\nChampions generan {champion_share:.1%} de las ventas")

# ─────────────────────────────────────────────
# 8. TABLA AGREGADA POR CLIENTE
# ─────────────────────────────────────────────
ventas_cliente = df.groupby("ClienteID").agg(

    Ventas_Total=("VENTA NETA", "sum"),
    Tickets=("DOCUMENTO_NORMALIZADO", "nunique"),
    Productos=("COD. PROD", "nunique"),

    Primera_Compra=("FECHA", "min"),
    Ultima_Compra=("FECHA", "max")

).reset_index()

ventas_cliente["Antiguedad_Cliente_Dias"] = (
    ventas_cliente["Ultima_Compra"] -
    ventas_cliente["Primera_Compra"]
).dt.days

ventas_cliente["Ticket_Promedio"] = (
    ventas_cliente["Ventas_Total"] /
    ventas_cliente["Tickets"]
)

# Convertir fechas para Power BI
ventas_cliente["Primera_Compra"] = ventas_cliente["Primera_Compra"].dt.strftime("%Y-%m-%d")
ventas_cliente["Ultima_Compra"]  = ventas_cliente["Ultima_Compra"].dt.strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
# 9. EXPORTAR ARCHIVOS
# ─────────────────────────────────────────────
rfm.to_csv(
    "Dim_Cliente_RFM.csv",
    index=False,
    encoding="utf-8-sig"
)

ventas_cliente.to_csv(
    "Fact_Cliente_Agregado.csv",
    index=False,
    encoding="utf-8-sig"
)

resumen.to_csv(
    "Resumen_RFM_Segmentos.csv",
    encoding="utf-8-sig"
)
rfm.to_csv("C:/Users/HP/Downloads/Dim_Cliente_RFM.csv", index=False, encoding="utf-8-sig")

ventas_cliente.to_csv("C:/Users/HP/Downloads/Fact_Cliente_Agregado.csv", index=False, encoding="utf-8-sig")

resumen.to_csv("C:/Users/HP/Downloads/Resumen_RFM_Segmentos.csv", encoding="utf-8-sig")

print("\nArchivos generados correctamente:")
print("Dim_Cliente_RFM.csv")
print("Fact_Cliente_Agregado.csv")
print("Resumen_RFM_Segmentos.csv")