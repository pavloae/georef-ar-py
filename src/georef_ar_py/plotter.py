import logging

import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
import numpy as np
import pandas as pd
import pyproj
from shapely import Point, LineString, Polygon

logger = logging.getLogger(__name__)

def set_plot_limits(gdf_3857, ax):
    x_min, y_min, x_max, y_max = gdf_3857.total_bounds
    margin = min(200, .1 * (x_max - x_min), .1 * (y_max - y_min))
    ax.set_xlim(x_min - margin, x_max + margin)
    ax.set_ylim(y_min - margin, y_max + margin)

def plot_base(ax):
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs='EPSG:3857')

def plot_geometries(gdf_4326, ax=None, label=None):

    gdf_3857 = gdf_4326.to_crs(epsg=3857)

    if not ax:
        fig, ax = plt.subplots(figsize=(12, 8))
        set_plot_limits(gdf_3857, ax)
        plot_base(ax)

    # Graficar puntos en rojo
    points = gdf_3857[gdf_3857.geometry.type == 'Point']
    if len(points) > 0:
        points.plot(ax=ax, color='red', markersize=50, label=label)

    # Graficar líneas en verde
    lines = gdf_3857[gdf_3857.geometry.type == 'LineString']
    if len(lines) > 0:
        lines.plot(ax=ax, color='green', linewidth=4, label=label)

    # Graficar polígonos en azul
    polygons = gdf_3857[gdf_3857.geometry.type == 'Polygon']
    if len(polygons) > 0:
        polygons.plot(ax=ax, edgecolor='blue', facecolor='none', linewidth=2, label=label)

    # Añadir etiquetas con el número de registro
    for idx, row in gdf_3857.iterrows():
        if isinstance(row.geometry, Point):
            x, y = row.geometry.x, row.geometry.y
        elif isinstance(row.geometry, LineString):
            x, y = row.geometry.xy  # Para líneas, usamos el primer punto
            x, y = x[0], y[0]
        elif isinstance(row.geometry, Polygon):
            x, y = row.geometry.centroid.x, row.geometry.centroid.y  # Usar el centro del polígono
        else:
            continue

        ax.text(x, y, str(idx + 1), fontsize=18, ha='left', va='bottom')

    return ax

def calculate_ticks_degrees(min_val, max_val, min_ticks=3, max_ticks=7):
    possible_steps = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    ideal_step = (max_val - min_val) / ((min_ticks + max_ticks) / 2)
    tick_step = min(possible_steps, key=lambda x: abs(x - ideal_step))
    ticks = np.arange(np.floor(min_val / tick_step) * tick_step,
                      np.ceil(max_val / tick_step) * tick_step + tick_step, tick_step)
    return ticks[1:-1]

def plot_ticks(gdf_4326, ax):
    lon_ticks = calculate_ticks_degrees(gdf_4326.total_bounds[0], gdf_4326.total_bounds[2])
    lat_ticks = calculate_ticks_degrees(gdf_4326.total_bounds[1], gdf_4326.total_bounds[3])

    # Proyección de EPSG:3857 a EPSG:4326
    mercator_to_wgs84 = pyproj.CRS('EPSG:3857')
    wgs84 = pyproj.CRS('EPSG:4326')
    project = pyproj.Transformer.from_crs(wgs84, mercator_to_wgs84, always_xy=True)
    x_ticks = [project.transform(lon, lat_ticks[0])[0] for lon in lon_ticks]
    y_ticks = [project.transform(lon_ticks[0], lat)[1] for lat in lat_ticks]

    # Agregar los ticks transformados en el gráfico, las etiquetas y las líneas guías
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([f'{lon:g}°' for lon in lon_ticks])
    ax.vlines(x_ticks, ymin=ax.get_ylim()[0], ymax=ax.get_ylim()[1], colors='gray', linestyles='dashed',
              alpha=0.5)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels([f'{lat:g}°' for lat in lat_ticks])
    ax.hlines(y_ticks, xmin=ax.get_xlim()[0], xmax=ax.get_xlim()[1], colors='gray', linestyles='dashed',
              alpha=0.5)

def plot_csv_points(input_file, output_file=None, lon_head="lon", lat_head="lat", label=None):

    df = pd.read_csv(input_file)
    df['geometry_type'] = 'Point'
    df['geometry'] = df.apply(lambda row: Point(row[lon_head], row[lat_head]), axis=1)

    gdf_4326 = gpd.GeoDataFrame(df, geometry='geometry')
    gdf_4326.crs = "EPSG:4326"

    ax = plot_geometries(gdf_4326, label=label)

    plot_ticks(gdf_4326, ax)

    plt.title(f'{label} desde {input_file}')
    if label:
        plt.legend()
    plt.savefig(output_file)
    logger.info("Archivo guardado en: %s", output_file)
