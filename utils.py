import json
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from matplotlib.colors import Normalize
from matplotlib import cm
from config import MAPBOX_APIKEY

class MakeGeoMap:

    def __init__(self, metric, cmap):
        self.regions_json =  self.regions_json('italy-regions.json')
        self.len_regions = len(self.regions_json['features'])
        self.metric = metric
        self.cmap = cmap

    @staticmethod
    def regions_json(filename):
        """
        Reutnr the geojson file
        """
        with open(filename) as f:
             return json.load(f)

    def central_coords(self):
        """
        Return the central lats and lons for each region
        """
        lons=[]
        lats=[]
        for k in range(self.len_regions):
            geometry = self.regions_json['features'][k]['geometry']

            if geometry['type'] == 'Polygon':
                county_coords=np.array(geometry['coordinates'][0])
            elif geometry['type'] == 'MultiPolygon':
                county_coords=np.array(geometry['coordinates'][0][0])

            m0, M0 =county_coords[:,0].min(), county_coords[:,0].max()
            m1, M1 =county_coords[:,1].min(), county_coords[:,1].max()
            lons.append(0.5*(m0+M0))
            lats.append(0.5*(m1+M1))

        return lons, lats

    def regions_names(self):
        return [self.regions_json['features'][k]['properties']['name'] for k in range(self.len_regions)]

    def match_regions(self):
        """
        Match the region names of self.metric with the region names of the geojson
        using fuzzy matching.

        Return a dict with {'old_name': 'new_name'}

        """
        l = []
        for r1 in self.regions_names():
            for r2 in self.metric.index:
                r11 = r1.replace('-', ' ')
                r22 = r2.replace('-', ' ').lower()
                l.append([r1,r2,fuzz.ratio(r11, r22)])

        matched = np.array([x for x in l if x[2] > 75])

        return {key: value for (key, value) in matched[:,[1,0]]}

    def reindexed_dataframe(self):
        """
        Return a reindexed dataframe with the region names from geojson
        """
        tmp = self.metric
        tmp.index = tmp.index.map(self.match_regions())
        #give the same index order as the geojson
        return tmp.reindex(index = self.regions_names())

    def make_source(self):
        """
        Return a list of dictionaries needed by mapbox to render the regions layouts
        """
        sources = []
        for feature in self.regions_json['features']:
            sources.append(dict(type= 'FeatureCollection', features = [feature]))
        return sources

    def get_colors(self):
        """
        Return a list of normalized rgba colors for the regions
        """
        df = self.reindexed_dataframe()

        colormap = cm.get_cmap(self.cmap)
        norm = Normalize(vmin=df.min(), vmax=df.max())

        sm = cm.ScalarMappable(norm=norm, cmap=colormap)

        return  ['rgba' + str(sm.to_rgba(m, bytes = True, alpha = 0.8)) if not np.isnan(m) else 'rgba(128,128,128,1)'  for m in df.values]

    def make_figure(self, title, ispercentage = True):
        """
        Return the data and the layout for plotly
        """
        df = self.reindexed_dataframe()
        lons,lats = self.central_coords()

        facecolor = self.get_colors()

        text_value = df
        tickformat = ""

        if ispercentage:
            text_value = (df * 100).round(2).astype(str) + "%"
            tickformat = ".0%"

        text = ['{} <br> {}'.format(r,v) for r, v in zip(df.index, text_value)]

        source = self.make_source()

        TheMap = dict(type='scattermapbox',
                     lat=lats,
                     lon=lons,
                     mode='markers',
                     text=text,
                     marker=dict(size=1,
                                 color=facecolor,
                                 showscale = True,
                                 cmin = df.min(),
                                 cmax = df.max(),
                                 colorscale = self.cmap,
                                 reversescale = True,
                                 colorbar = dict(tickformat = tickformat )
                                 ),
                     showlegend=False,
                     hoverinfo='text'
                     )

        layers=([dict(sourcetype = 'geojson',
                source =source[k],
                below="",
                type = 'line',
                line = dict(width = 1),
                color = 'black',
                ) for k in range(self.len_regions)] +

                [dict(sourcetype = 'geojson',
                     source =source[k],
                     below="water",
                     type = 'fill',
                     color = facecolor[k],
                     opacity=0.8
                    ) for k in range(self.len_regions)]
                )


        layout = dict(title=title,
              autosize=False,
              width=700,
              height=800,
              hovermode='closest',
              hoverdistance = 30,

              mapbox=dict(accesstoken=MAPBOX_APIKEY,
                          layers=layers,
                          bearing=0,
                          center=dict(
                          lat=41.871941,
                          lon=12.567380),
                          pitch=0,
                          zoom=4.9,
                          style = 'light'
                    )
              )

        return TheMap, layout
