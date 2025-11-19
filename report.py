#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Análise de Terrenos
Mivita - Processamento Geoespacial e Cálculos
"""

import geopandas as gpd
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
import logging

logger = logging.getLogger(__name__)


class TerrainAnalyzer:
    """Classe para análise e processamento de terrenos"""
    
    def __init__(self, geodataframe):
        """
        Inicializa o analisador com um GeoDataFrame
        
        Args:
            geodataframe: GeoDataFrame com um ou mais lotes
        """
        self.gdf = geodataframe
        self.unified_geometry = None
    
    def unify_lots(self):
        """
        Une múltiplos lotes em uma única geometria
        
        Returns:
            Shapely geometry com a união dos lotes
        """
        try:
            geometries = self.gdf.geometry.tolist()
            
            if len(geometries) == 1:
                self.unified_geometry = geometries[0]
            else:
                self.unified_geometry = unary_union(geometries)
            
            logger.info(f"Unidos {len(geometries)} lotes")
            return self.unified_geometry
        
        except Exception as e:
            logger.error(f"Erro ao unir lotes: {str(e)}")
            raise
    
    def calculate_area(self):
        """
        Calcula área total em m² (considerando projeção apropriada)
        
        Returns:
            float: Área em metros quadrados
        """
        try:
            # Se ainda não unificou, unificar
            if self.unified_geometry is None:
                self.unify_lots()
            
            # Converter para projeção métrica (UTM zone 24S para Espírito Santo)
            gdf_metric = self.gdf.to_crs("EPSG:31984")  # SIRGAS 2000 / UTM zone 24S
            
            if len(gdf_metric) == 1:
                area = gdf_metric.geometry.iloc[0].area
            else:
                unified_metric = unary_union(gdf_metric.geometry.tolist())
                area = unified_metric.area
            
            return round(area, 2)
        
        except Exception as e:
            logger.warning(f"Erro ao calcular área em projeção métrica: {str(e)}")
            # Fallback: usar área em graus (menos preciso)
            if self.unified_geometry is None:
                self.unify_lots()
            # Aproximação: 1 grau ≈ 111km no equador
            area_deg = self.unified_geometry.area
            area_m2 = area_deg * (111320 ** 2)  # Conversão aproximada
            return round(area_m2, 2)
    
    def calculate_perimeter(self):
        """
        Calcula perímetro total em metros
        
        Returns:
            float: Perímetro em metros
        """
        try:
            if self.unified_geometry is None:
                self.unify_lots()
            
            # Converter para projeção métrica
            gdf_metric = self.gdf.to_crs("EPSG:31984")
            
            if len(gdf_metric) == 1:
                perimeter = gdf_metric.geometry.iloc[0].length
            else:
                unified_metric = unary_union(gdf_metric.geometry.tolist())
                perimeter = unified_metric.length
            
            return round(perimeter, 2)
        
        except Exception as e:
            logger.warning(f"Erro ao calcular perímetro: {str(e)}")
            if self.unified_geometry is None:
                self.unify_lots()
            # Aproximação
            perimeter_deg = self.unified_geometry.length
            perimeter_m = perimeter_deg * 111320
            return round(perimeter_m, 2)
    
    def extract_zoning_info(self):
        """
        Extrai informações de zoneamento dos lotes
        
        Returns:
            dict: Dicionário com informações de zoneamento
        """
        zoning_info = {}
        
        try:
            # Buscar colunas relacionadas a zoneamento
            zoning_columns = [
                'zona', 'zoneamento', 'zone', 'ZONA', 'ZONEAMENTO',
                'uso', 'uso_solo', 'tipo_zona', 'categoria'
            ]
            
            for col in zoning_columns:
                if col in self.gdf.columns:
                    # Pegar valores únicos
                    values = self.gdf[col].dropna().unique().tolist()
                    if values:
                        zoning_info[col] = values[0] if len(values) == 1 else values
            
            # Buscar coeficientes e parâmetros urbanísticos
            param_columns = [
                'coeficiente', 'ca', 'coef_aproveitamento', 'taxa_ocupacao',
                'to', 'gabarito', 'altura_max', 'recuo', 'testada_min'
            ]
            
            for col in param_columns:
                if col in self.gdf.columns:
                    values = self.gdf[col].dropna().tolist()
                    if values:
                        zoning_info[col] = values[0] if len(values) == 1 else values
            
            if not zoning_info:
                zoning_info['status'] = 'Informações de zoneamento não disponíveis nos dados'
            
            return zoning_info
        
        except Exception as e:
            logger.error(f"Erro ao extrair informações de zoneamento: {str(e)}")
            return {'error': str(e)}
    
    def calculate_info(self):
        """
        Calcula todas as informações do(s) lote(s)
        
        Returns:
            dict: Dicionário completo com todas as informações
        """
        try:
            area = self.calculate_area()
            perimeter = self.calculate_perimeter()
            zoning = self.extract_zoning_info()
            
            # Informações adicionais dos lotes
            additional_info = {}
            
            # Tentar extrair outras informações relevantes
            info_columns = [
                'matricula', 'inscricao', 'proprietario', 'endereco',
                'logradouro', 'numero', 'quadra', 'lote'
            ]
            
            for col in info_columns:
                if col in self.gdf.columns:
                    values = self.gdf[col].dropna().unique().tolist()
                    if values:
                        additional_info[col] = values
            
            info = {
                'total_lotes': len(self.gdf),
                'area_total_m2': area,
                'area_total_hectares': round(area / 10000, 4),
                'perimetro_m': perimeter,
                'zoneamento': zoning,
                'informacoes_adicionais': additional_info if additional_info else 'Não disponível'
            }
            
            logger.info(f"Informações calculadas: {area:.2f} m²")
            return info
        
        except Exception as e:
            logger.error(f"Erro ao calcular informações: {str(e)}")
            return {
                'error': str(e),
                'total_lotes': len(self.gdf)
            }
    
    def get_centroid(self):
        """
        Calcula o centroide da geometria unificada
        
        Returns:
            tuple: (longitude, latitude)
        """
        try:
            if self.unified_geometry is None:
                self.unify_lots()
            
            centroid = self.unified_geometry.centroid
            return (centroid.x, centroid.y)
        
        except Exception as e:
            logger.error(f"Erro ao calcular centroide: {str(e)}")
            return None
    
    def get_bounds(self):
        """
        Retorna os limites (bounding box) da geometria
        
        Returns:
            tuple: (minx, miny, maxx, maxy)
        """
        try:
            if self.unified_geometry is None:
                self.unify_lots()
            
            return self.unified_geometry.bounds
        
        except Exception as e:
            logger.error(f"Erro ao obter limites: {str(e)}")
            return None


class ZoningCalculator:
    """Classe para cálculos específicos de potencial construtivo"""
    
    def __init__(self, area_terreno, zoning_params):
        """
        Args:
            area_terreno: Área do terreno em m²
            zoning_params: Dicionário com parâmetros urbanísticos
        """
        self.area_terreno = area_terreno
        self.params = zoning_params
    
    def calculate_buildable_area(self):
        """
        Calcula área edificável máxima
        
        Returns:
            float: Área edificável em m²
        """
        try:
            ca = self.params.get('coeficiente', self.params.get('ca', 1.0))
            
            if isinstance(ca, (list, tuple)):
                ca = ca[0]
            
            ca = float(ca)
            buildable = self.area_terreno * ca
            
            return round(buildable, 2)
        
        except Exception as e:
            logger.warning(f"Erro ao calcular área edificável: {str(e)}")
            return None
    
    def calculate_footprint(self):
        """
        Calcula área de projeção no solo (taxa de ocupação)
        
        Returns:
            float: Área de projeção em m²
        """
        try:
            to = self.params.get('taxa_ocupacao', self.params.get('to', 0.5))
            
            if isinstance(to, (list, tuple)):
                to = to[0]
            
            to = float(to)
            footprint = self.area_terreno * to
            
            return round(footprint, 2)
        
        except Exception as e:
            logger.warning(f"Erro ao calcular taxa de ocupação: {str(e)}")
            return None