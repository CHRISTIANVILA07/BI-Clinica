"""
=============================================================
  FORECAST DE VENTAS MENSUALES — BI DERMATOLÓGICO
=============================================================
  Input : Fact_Lineas_Limpia.csv
  Output: Forecast_Ventas_Mensual.csv
          Forecast_Por_Sede.csv

  Instalar dependencias:
    pip install prophet pandas numpy
=============================================================
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────
# CONFIGURACIÓN 
# ─────────────────────────────────────────────────────────
RUTA_INPUT  = r"C:\Users\HP\Downloads\Fact_Lineas_Limpia.csv"
RUTA_OUTPUT = r"C:\Users\HP\Downloads\Forecasts"
# ─────────────────────────────────────────────────────────

N_TEST     = 3
N_FORECAST = 6

os.makedirs(RUTA_OUTPUT, exist_ok=True)
print(f"Archivos se guardaran en: {RUTA_OUTPUT}")

# ─────────────────────────────────────────────
# 1. CARGAR Y LIMPIAR
# ─────────────────────────────────────────────
df = pd.read_csv(RUTA_INPUT, encoding="utf-8-sig")
df.columns = df.columns.str.strip()

df["FECHA"]      = pd.to_datetime(df["FECHA"], dayfirst=True, errors="coerce")
df["VENTA NETA"] = pd.to_numeric(df["VENTA NETA"], errors="coerce")

# FIX 1: excluir filas sin fecha valida, sin año, y datos sucios
df = df.dropna(subset=["FECHA"])
df = df[df["FECHA"].dt.year >= 2024]
df = df[df["VENTA NETA"] > 0]

# FIX 2: no aplicar filtro por monto — los outliers de tipeo son pocos
# y su impacto en la suma mensual (~24K filas) es despreciable.
# Solo excluimos negativos (ya filtrado arriba con VENTA NETA > 0)
print(f"  Sin filtro de outliers — se usan todos los {len(df):,} registros positivos")

print(f"Registros validos: {len(df):,}")
df["MesFecha"] = df["FECHA"].dt.to_period("M").dt.to_timestamp()

# FIX 3: solo usar meses completos para entrenamiento
# 2026 tiene meses parciales — excluirlos del dataset de entrenamiento
# (el modelo solo usa hasta dic 2025, que son 24 meses completos)
ULTIMO_MES_COMPLETO = pd.Timestamp("2025-12-01")
df_train = df[df["MesFecha"] <= ULTIMO_MES_COMPLETO]
print(f"Periodo de entrenamiento: {df_train['MesFecha'].min().strftime('%Y-%m')} a {df_train['MesFecha'].max().strftime('%Y-%m')}")

# ─────────────────────────────────────────────
# 2. FUNCION DE FORECAST
# ─────────────────────────────────────────────
def hacer_forecast(serie_mensual, nombre="Total", n_test=3, n_forecast=6):
    serie = serie_mensual.copy().sort_values("Fecha").reset_index(drop=True)

    # FIX 2: excluir meses incompletos
    umbral = serie["VentaNeta"].mean() * 0.1
    serie = serie[serie["VentaNeta"] > umbral].reset_index(drop=True)

    if len(serie) < 12:
        print(f"  {nombre}: solo {len(serie)} meses — insuficiente")
        return None

    train = serie.iloc[:-n_test].copy()
    test  = serie.iloc[-n_test:].copy()
    print(f"\n  {nombre}: {len(serie)} meses | train hasta {train['Fecha'].max().strftime('%Y-%m')}")

    try:
        from prophet import Prophet

        train_p = train.rename(columns={"Fecha": "ds", "VentaNeta": "y"})

        # FIX 3: seasonality_mode additive para datos estables
        model = Prophet(
            yearly_seasonality       = True,
            weekly_seasonality       = False,
            daily_seasonality        = False,
            seasonality_mode         = "additive",
            interval_width           = 0.80,
            changepoint_prior_scale  = 0.2
        )
        model.fit(train_p)

        future = model.make_future_dataframe(periods=n_test + n_forecast, freq="MS")
        pred   = model.predict(future)

        # Validacion
        pred_test = pred[pred["ds"].isin(test["Fecha"])][["ds","yhat"]].copy()
        pred_test.columns = ["Fecha","pred"]
        merged = test.merge(pred_test, on="Fecha")
        mae  = np.mean(np.abs(merged["VentaNeta"] - merged["pred"]))
        mape = np.mean(np.abs((merged["VentaNeta"] - merged["pred"]) / merged["VentaNeta"])) * 100
        print(f"  MAE: S/ {mae:,.0f} | MAPE: {mape:.1f}% {'OK' if mape < 20 else 'revisar'}")

        ultimo_real = serie["Fecha"].max()
        fut = pred[pred["ds"] > ultimo_real].head(n_forecast)[["ds","yhat","yhat_lower","yhat_upper"]].copy()
        fut.columns = ["Fecha","VentaNeta","Lower","Upper"]
        fut["Tipo"]   = "Forecast"
        MODEL_USED    = "Prophet"

    except ImportError:
        print(f"  Prophet no instalado — usando Holt-Winters")

        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        sp = min(12, len(train) // 2)
        m  = ExponentialSmoothing(train.set_index("Fecha")["VentaNeta"],
                                  trend="add", seasonal="add" if sp>=4 else None,
                                  seasonal_periods=sp).fit()

        pred_test_v = m.forecast(n_test)
        mae  = np.mean(np.abs(test["VentaNeta"].values - pred_test_v.values))
        mape = np.mean(np.abs((test["VentaNeta"].values - pred_test_v.values) / test["VentaNeta"].values)) * 100
        print(f"  MAE: S/ {mae:,.0f} | MAPE: {mape:.1f}%")

        m2   = ExponentialSmoothing(serie.set_index("Fecha")["VentaNeta"],
                                    trend="add", seasonal="add" if sp>=4 else None,
                                    seasonal_periods=sp).fit()
        pf   = m2.forecast(n_forecast)
        fechas_fut = pd.date_range(
            start=serie["Fecha"].max() + pd.offsets.MonthBegin(1),
            periods=n_forecast, freq="MS"
        )
        fut = pd.DataFrame({
            "Fecha"    : fechas_fut,
            "VentaNeta": pf.values.clip(0),
            "Lower"    : pf.values * 0.85,
            "Upper"    : pf.values * 1.15,
            "Tipo"     : "Forecast"
        })
        MODEL_USED = "Holt-Winters"

    reales = serie.copy()
    reales["Tipo"]  = "Real"
    reales["Lower"] = np.nan
    reales["Upper"] = np.nan

    resultado = pd.concat([
        reales[["Fecha","VentaNeta","Tipo","Lower","Upper"]],
        fut[["Fecha","VentaNeta","Tipo","Lower","Upper"]]
    ], ignore_index=True)

    resultado["VentaNeta"] = resultado["VentaNeta"].clip(lower=0).round(2)
    resultado["Lower"]     = resultado["Lower"].clip(lower=0).round(2)
    resultado["Upper"]     = resultado["Upper"].round(2)
    resultado["Serie"]     = nombre
    resultado["Modelo"]    = MODEL_USED

    return resultado.sort_values("Fecha").reset_index(drop=True)


# ─────────────────────────────────────────────
# 3. FORECAST TOTAL
# ─────────────────────────────────────────────
print("\n" + "="*50)
print("FORECAST TOTAL")
print("="*50)

mensual = df_train.groupby("MesFecha")["VENTA NETA"].sum().reset_index()
mensual.columns = ["Fecha","VentaNeta"]

ft = hacer_forecast(mensual, nombre="TOTAL")
if ft is not None:
    path = os.path.join(RUTA_OUTPUT, "Forecast_Ventas_Mensual.csv")
    ft.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n  Guardado: {path}")
    print("\n  Proximos 6 meses:")
    print(ft[ft["Tipo"]=="Forecast"][["Fecha","VentaNeta","Lower","Upper"]].to_string(index=False))


# ─────────────────────────────────────────────
# 4. FORECAST POR SEDE
# ─────────────────────────────────────────────
print("\n" + "="*50)
print("FORECAST POR SEDE")
print("="*50)

resultados = []
for sede in sorted(df_train["SEDE"].dropna().unique()):
    mens = df_train[df_train["SEDE"]==sede].groupby("MesFecha")["VENTA NETA"].sum().reset_index()
    mens.columns = ["Fecha","VentaNeta"]
    r = hacer_forecast(mens, nombre=sede)
    if r is not None:
        resultados.append(r)

if resultados:
    fs = pd.concat(resultados, ignore_index=True)
    path = os.path.join(RUTA_OUTPUT, "Forecast_Por_Sede.csv")
    fs.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n  Guardado: {path}")


# ─────────────────────────────────────────────
# 5. RESUMEN
# ─────────────────────────────────────────────
print("\n" + "="*50)
print("ARCHIVOS GENERADOS")
print("="*50)
for f in sorted(os.listdir(RUTA_OUTPUT)):
    if f.endswith(".csv"):
        kb = os.path.getsize(os.path.join(RUTA_OUTPUT, f)) / 1024
        print(f"  {f}  ({kb:.1f} KB)")

print(f"\nCarpeta: {RUTA_OUTPUT}")

