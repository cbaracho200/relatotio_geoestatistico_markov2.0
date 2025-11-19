#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
App Flask para Gerenciamento de Prospecção de Terrenos
Mivita - Análise Geoespacial de Lotes Urbanos
"""

from flask import Flask, render_template, jsonify, request
import geopandas as gpd
from shapely import from_wkb
from shapely.geometry import Polygon
import pandas as pd
import json
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
import logging

# Importar módulo de relatórios
from report import TerrainAnalyzer

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inicializar Flask
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Variável global para armazenar dados
GEOJSON_DATA = None
GEOJSON_PATH = Path(__file__).parent / './data/renda_setores_vix.parquet'


def load_geojson():
    """Carrega o arquivo GeoJSON na inicialização"""
    global GEOJSON_DATA
    
    if not GEOJSON_PATH.exists():
        logger.error(f"ERRO: Arquivo GeoJSON não encontrado em {GEOJSON_PATH}")
        logger.error("Por favor, adicione o arquivo 'terrenos.geojson' na raiz do projeto")
        return None
    
    try:
        gdf = pd.read_parquet(GEOJSON_PATH)
        geo = from_wkb(gdf["geometry"].values)
        # Dados estão em EPSG:31984 (UTM zona 25S) - Vitória, ES
        gdf = gpd.GeoDataFrame(gdf.drop(columns="geometry"), geometry=geo, crs="EPSG:31984")
        gdf = gdf.to_crs(epsg=4326)
        
        logger.info(f"GeoJSON carregado com sucesso: {len(gdf)} features")
        logger.info(f"Colunas disponíveis: {list(gdf.columns)}")
        
        return gdf
    
    except Exception as e:
        logger.error(f"Erro ao carregar GeoJSON: {str(e)}")
        return None


@app.route('/')
def index():
    """Página principal"""
    return render_template('index.html')


@app.route('/api/map-bounds', methods=['GET'])
def get_map_bounds():
    """
    Retorna o centro e bounds dos dados para centralizar o mapa dinamicamente
    """
    if GEOJSON_DATA is None:
        return jsonify({
            'success': False,
            'error': 'Dados não carregados'
        }), 500
    
    try:
        # Calcular bounds de todos os dados
        bounds = GEOJSON_DATA.total_bounds  # [minx, miny, maxx, maxy]
        
        # Calcular centro
        center_lon = (bounds[0] + bounds[2]) / 2
        center_lat = (bounds[1] + bounds[3]) / 2
        
        logger.info(f"Bounds calculados: {bounds}, Centro: [{center_lat}, {center_lon}]")
        
        return jsonify({
            'success': True,
            'center': [center_lat, center_lon],
            'bounds': [
                [bounds[1], bounds[0]],  # southwest [lat, lon]
                [bounds[3], bounds[2]]   # northeast [lat, lon]
            ]
        })
    
    except Exception as e:
        logger.error(f"Erro ao calcular bounds: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/bairros', methods=['GET'])
def get_bairros():
    """Retorna lista de bairros únicos disponíveis"""
    if GEOJSON_DATA is None:
        return jsonify({
            'success': False,
            'error': 'Dados GeoJSON não carregados. Adicione terrenos.geojson na raiz do projeto.'
        }), 500
    
    try:
        # Identificar coluna de bairro (pode ser 'bairro', 'BAIRRO', 'nome', etc)
        bairro_col = None
        for col in ['bairro', 'BAIRRO', 'Bairro', 'nome', 'NOME', 'Nome', 'neighborhood']:
            if col in GEOJSON_DATA.columns:
                bairro_col = col
                break
        
        if bairro_col is None:
            logger.warning("Coluna de bairro não identificada automaticamente")
            return jsonify({
                'success': False,
                'error': 'Coluna de bairro não encontrada no GeoJSON',
                'available_columns': list(GEOJSON_DATA.columns)
            }), 400
        
        bairros = sorted(GEOJSON_DATA[bairro_col].dropna().unique().tolist())
        
        logger.info(f"Retornando {len(bairros)} bairros")
        return jsonify({
            'success': True,
            'bairros': bairros,
            'total': len(bairros)
        })
    
    except Exception as e:
        logger.error(f"Erro ao buscar bairros: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/lotes/<bairro>', methods=['GET'])
def get_lotes_by_bairro(bairro):
    """Retorna todos os lotes de um bairro específico"""
    if GEOJSON_DATA is None:
        return jsonify({
            'success': False,
            'error': 'Dados GeoJSON não carregados'
        }), 500
    
    try:
        # Identificar coluna de bairro
        bairro_col = None
        for col in ['bairro', 'BAIRRO', 'Bairro', 'nome', 'NOME', 'Nome', 'neighborhood']:
            if col in GEOJSON_DATA.columns:
                bairro_col = col
                break
        
        if bairro_col is None:
            return jsonify({
                'success': False,
                'error': 'Coluna de bairro não encontrada'
            }), 400
        
        # Filtrar lotes do bairro
        filtered = GEOJSON_DATA[GEOJSON_DATA[bairro_col] == bairro]
        
        if len(filtered) == 0:
            return jsonify({
                'success': False,
                'error': f'Nenhum lote encontrado para o bairro {bairro}'
            }), 404
        
        # Converter para GeoJSON
        geojson = json.loads(filtered.to_json())
        
        logger.info(f"Retornando {len(filtered)} lotes do bairro {bairro}")
        return jsonify({
            'success': True,
            'bairro': bairro,
            'total_lotes': len(filtered),
            'geojson': geojson
        })
    
    except Exception as e:
        logger.error(f"Erro ao buscar lotes: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/unir-lotes', methods=['POST'])
def unir_lotes():
    """Une múltiplos lotes selecionados e calcula informações"""
    if GEOJSON_DATA is None:
        return jsonify({
            'success': False,
            'error': 'Dados GeoJSON não carregados'
        }), 500
    
    try:
        data = request.get_json()
        indices = data.get('indices', [])
        
        if not indices or len(indices) == 0:
            return jsonify({
                'success': False,
                'error': 'Nenhum lote selecionado'
            }), 400
        
        # Filtrar lotes pelos índices
        selected_lots = GEOJSON_DATA.iloc[indices]
        
        # Criar analisador
        analyzer = TerrainAnalyzer(selected_lots)
        
        # Unir geometrias
        unified_geom = analyzer.unify_lots()
        
        # Calcular informações
        info = analyzer.calculate_info()
        
        # Converter geometria unida para GeoJSON
        unified_geojson = mapping(unified_geom)
        
        logger.info(f"Unidos {len(indices)} lotes - Área total: {info['area_total_m2']:.2f} m²")
        
        return jsonify({
            'success': True,
            'total_lotes_unidos': len(indices),
            'geometry': unified_geojson,
            'info': info
        })
    
    except Exception as e:
        logger.error(f"Erro ao unir lotes: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/info-lote/<int:index>', methods=['GET'])
def get_lote_info(index):
    """Retorna informações detalhadas de um lote específico"""
    if GEOJSON_DATA is None:
        return jsonify({
            'success': False,
            'error': 'Dados GeoJSON não carregados'
        }), 500
    
    try:
        if index < 0 or index >= len(GEOJSON_DATA):
            return jsonify({
                'success': False,
                'error': 'Índice de lote inválido'
            }), 400
        
        lote = GEOJSON_DATA.iloc[index]
        
        # Criar analisador para lote único
        analyzer = TerrainAnalyzer(GEOJSON_DATA.iloc[[index]])
        info = analyzer.calculate_info()
        
        return jsonify({
            'success': True,
            'info': info
        })
    
    except Exception as e:
        logger.error(f"Erro ao buscar informações do lote: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/report')
def report():
    """Página de relatório detalhado"""
    return render_template('report.html')


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Rota não encontrada'
    }), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Erro interno do servidor'
    }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("MIVITA - Sistema de Prospecção de Terrenos")
    print("=" * 60)
    
    # Carregar dados na inicialização
    GEOJSON_DATA = load_geojson()
    
    if GEOJSON_DATA is None:
        print("\n⚠️  ATENÇÃO: Arquivo GeoJSON não encontrado!")
        print(f"📁 Adicione o arquivo 'terrenos.geojson' em: {GEOJSON_PATH.parent}")
        print("\nO sistema iniciará, mas não funcionará sem os dados.\n")
    else:
        print(f"\n✅ Dados carregados: {len(GEOJSON_DATA)} features")
        print(f"📊 Colunas: {', '.join(GEOJSON_DATA.columns[:5])}{'...' if len(GEOJSON_DATA.columns) > 5 else ''}")
    
    print("\n🌐 Acesse: http://localhost:5000")
    print("=" * 60 + "\n")
    

    app.run(debug=True, host='0.0.0.0', port=5003)
