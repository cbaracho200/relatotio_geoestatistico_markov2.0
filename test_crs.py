#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para diagnosticar o CRS correto dos dados
"""

import pandas as pd
import geopandas as gpd
from shapely import from_wkb
from pathlib import Path

# Carregar dados
data_path = Path(__file__).parent / './data/PDU_VIX.parquet'
df = pd.read_parquet(data_path)

print("=" * 60)
print("DIAGNÓSTICO DE CRS")
print("=" * 60)

# Converter WKB para geometria
geo = from_wkb(df["geometry"].values)
gdf_test = gpd.GeoDataFrame(df.drop(columns="geometry"), geometry=geo)

# Pegar um ponto de exemplo
sample_geom = gdf_test.geometry.iloc[0]
centroid = sample_geom.centroid

print(f"\n[*] COORDENADAS ORIGINAIS (sem CRS definido):")
print(f"   X (longitude/easting): {centroid.x:.6f}")
print(f"   Y (latitude/northing): {centroid.y:.6f}")

# Testar diferentes CRS
print("\n[*] TESTANDO DIFERENTES CRS:\n")

# Teste 1: Se dados estão em EPSG:31983 (UTM 24S - Vitória)
print("[1] EPSG:31983 (UTM 24S) -> EPSG:4326:")
gdf1 = gpd.GeoDataFrame(df.drop(columns="geometry"), geometry=geo, crs="EPSG:31983")
gdf1_wgs = gdf1.to_crs(epsg=4326)
c1 = gdf1_wgs.geometry.iloc[0].centroid
print(f"   Lat: {c1.y:.6f}, Lon: {c1.x:.6f}")
print(f"   Bounds: {gdf1_wgs.total_bounds}")

# Teste 2: Se dados estão em EPSG:31982 (UTM 23S - mais a oeste)
print("\n[2] EPSG:31982 (UTM 23S) -> EPSG:4326:")
gdf2 = gpd.GeoDataFrame(df.drop(columns="geometry"), geometry=geo, crs="EPSG:31982")
gdf2_wgs = gdf2.to_crs(epsg=4326)
c2 = gdf2_wgs.geometry.iloc[0].centroid
print(f"   Lat: {c2.y:.6f}, Lon: {c2.x:.6f}")
print(f"   Bounds: {gdf2_wgs.total_bounds}")

# Teste 3: Se dados já estão em EPSG:4326 (lat/lon WGS84)
print("\n[3] Ja em EPSG:4326 (WGS84 - sem conversao):")
gdf3 = gpd.GeoDataFrame(df.drop(columns="geometry"), geometry=geo, crs="EPSG:4326")
c3 = gdf3.geometry.iloc[0].centroid
print(f"   Lat: {c3.y:.6f}, Lon: {c3.x:.6f}")
print(f"   Bounds: {gdf3.total_bounds}")

# Teste 4: EPSG:31984 (UTM 25S - mais a leste)
print("\n[4] EPSG:31984 (UTM 25S) -> EPSG:4326:")
gdf4 = gpd.GeoDataFrame(df.drop(columns="geometry"), geometry=geo, crs="EPSG:31984")
gdf4_wgs = gdf4.to_crs(epsg=4326)
c4 = gdf4_wgs.geometry.iloc[0].centroid
print(f"   Lat: {c4.y:.6f}, Lon: {c4.x:.6f}")
print(f"   Bounds: {gdf4_wgs.total_bounds}")

print("\n" + "=" * 60)
print("[REF] REFERENCIA: Vitoria, ES")
print("   Latitude: ~-20.28")
print("   Longitude: ~-40.29")
print("=" * 60)

print("\n[OK] RESULTADO:")
print("   Compare as coordenadas acima com a referencia de Vitoria")
print("   O CRS correto e aquele que resulta em lon aprox -40.29\n")
