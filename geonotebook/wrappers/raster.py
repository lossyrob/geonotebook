import collections
import os
import re

import numpy as np

import pkg_resources as pr

from shapely.geometry import Polygon

class RddRasterData(object):

    def __init__(self, rdd, name=None):
        from geopyspark.geotrellis.rdd import RasterRDD, TiledRasterRDD
        from geopyspark.geotrellis.render import PngRDD

        if not (isinstance(rdd, RasterRDD) or isinstance(rdd, TiledRasterRDD) or isinstance(rdd, PngRDD)):
            raise TypeError("Expected a RasterRDD, TiledRasterRDD, or PngRDD")

        self.rdd = rdd

        if not name:
            self.name = str(hash(rdd))
        else:
            self.name = name

class GeoTrellisCatalogLayerData(object):

    def __init__(self,
                 geopysc,
                 catalog_uri,
                 layer_names,
                 key_type = None,
                 tile_type = None,
                 options = None,
                 name = None):
        from geopyspark.geotrellis.catalog import (_construct_catalog,
                                                   _mapped_cached,
                                                   get_layer_ids)
        from geopyspark.geotrellis.constants import SPATIAL, TILE

        if not key_type:
            rdd_type = SPAITAL
        if not tile_type:
            tile_type = TILE
        _construct_catalog(geopysc, catalog_uri, options)
        catalog = _mapped_cached[catalog_uri]
        self.catalog_uri = catalog_uri
        self.value_reader = catalog.value_reader
        self.key_type =  geopysc.map_key_input(key_type, True)
        self.tile_type = tile_type
        self.avroregistry = geopysc.avroregistry

        self.is_multi_layer = not isinstance(layer_names, str)
        if not self.is_multi_layer:
            self.layer_names = [layer_names]
        else:
            self.layer_names = layer_names

        # Get the max zoom
        layer_ids = list(get_layer_ids(geopysc, catalog_uri))
        zooms = { }
        for lid in layer_ids:
            ln = lid['name']
            if ln in self.layer_names:
                if not ln in zooms:
                    zooms[ln] = []
                zooms[ln].append(int(lid['zoom']))

        self.layer_zoom_ranges = {}
        for ln in zooms:
            self.layer_zoom_ranges[ln] = (min(zooms[ln]), max(zooms[ln]))
        self.max_zoom = min(map(lambda x: x[1], self.layer_zoom_ranges.values()))

        if not name:
            self.name = ','.join(self.layer_names)
        else:
            self.name = name


class RasterData(collections.Sequence):
    _default_schema = 'file'

    _schema_parser = re.compile(r'^(.*?)://.*$')

    _concrete_schema = {}

    @classmethod
    def register(cls, name, concrete_class):
        # TODO: some kind of validation on the API provided
        #       by the object from ep.load?
        cls._concrete_schema[name] = concrete_class

    @classmethod
    def discover_concrete_types(cls):
        for ep in pr.iter_entry_points(
                group='geonotebook.wrappers.raster_schema'):
            cls.register(ep.name, ep.load())

    @classmethod
    def is_valid(cls, uri):
        scheme = cls._schema_parser.match(uri)

        if scheme is None:
            return False
        else:
            return True

    def __init__(self, uri, indexes=None):
        try:

            scheme = self._schema_parser.match(uri)

            if scheme is None:
                scheme = self._default_schema
            else:
                scheme = scheme.group(1)

            self.reader = self._concrete_schema[scheme](uri)

        except KeyError:
            raise NotImplementedError(
                "{} cannot parse files of type '{}'".format(
                    self.__class__.__name__, scheme))
        except AttributeError:
            raise RuntimeError('Must pass in URI with schema.')

        self.band_indexes = range(1, self.reader.count + 1) \
            if indexes is None else indexes

        assert not min(self.band_indexes) < 1, \
            IndexError("Bands are indexed from 1")

        assert not max(self.band_indexes) > self.reader.count, \
            IndexError("Band index out of range")

    def index(self, *args, **kwargs):
        return self.reader.index(*args, **kwargs)

    def subset(self, annotation, **kwargs):
        return annotation.subset(self, **kwargs)

    def ix(self, x, y):
        if len(self) == 1:
            return self.reader.get_band_ix(self.band_indexes, x, y)[0]
        else:
            return self.reader.get_band_ix(self.band_indexes, x, y)

    def get_data(self, window=None, masked=True, axis=2, **kwargs):
        if len(self) == 1:
            return self.reader.get_band_data(self.band_indexes[0],
                                             window=window,
                                             maksed=masked,
                                             **kwargs)
        else:
            if masked:
                # TODO: fix masked array hack here
                # Note that this also assumes nodata for first band is the
                # same for all other bands.  all around kind of a hack
                kwargs["masked"] = False
                return np.ma.masked_values(
                    np.stack([
                        self.reader.get_band_data(i, window=window, **kwargs)
                        for i in self.band_indexes
                    ], axis=axis), self.nodata)
            else:
                return np.stack([self.reader.get_band_data(i, window=window,
                                                           masked=masked,
                                                           **kwargs)
                                 for i in self.band_indexes], axis=axis)

    def __len__(self):
        return len(self.band_indexes)

    def __getitem__(self, keys):
        if isinstance(keys, int):
            return RasterData(self.uri, indexes=[keys])
        elif all([isinstance(k, int) for k in keys]):
            return RasterData(self.uri, indexes=keys)
        else:
            raise IndexError(
                "Bands may only be indexed by an int or a list of ints"
            )

    @property
    def min(self):
        if len(self) == 1:
            return self.reader.get_band_min(self.band_indexes[0])
        else:
            return [self.reader.get_band_min(i) for i in self.band_indexes]

    @property
    def max(self):
        if len(self) == 1:
            return self.reader.get_band_max(self.band_indexes[0])
        else:
            return [self.reader.get_band_max(i) for i in self.band_indexes]

    @property
    def mean(self):
        if len(self) == 1:
            return self.reader.get_band_mean(self.band_indexes[0])
        else:
            return [self.reader.get_band_mean(i) for i in self.band_indexes]

    @property
    def stddev(self):
        if len(self) == 1:
            return self.reader.get_band_stddev(self.band_indexes[0])
        else:
            return [self.reader.get_band_stddev(i) for i in self.band_indexes]

    @property
    def nodata(self):
        # HACK,  we assume first band index's nodata is same
        # for all bands
        return self.reader.get_band_nodata(self.band_indexes[0])

    @property
    def shape(self):
        ulx, uly, lrx, lry = self.reader.bounds
        return Polygon([
            (ulx, uly),
            (lrx, uly),
            (lrx, lry),
            (ulx, lry),
            (ulx, uly)])

    @property
    def count(self):
        return self.reader.count

    @property
    def uri(self):
        return self.reader.uri

    @property
    def name(self):
        return os.path.splitext(os.path.basename(self.uri))[0]


RasterData.discover_concrete_types()


class RasterDataCollection(collections.Sequence):
    def __init__(self, items, verify=True, indexes=None):

        if verify:
            assert len(set([RasterData(i).count for i in items])) == 1, \
                "Not all items have the same number of bands!"

        self._items = items

        # All band counts will be the same unless verify=False
        # in which case you've made your own bed.
        band_count = RasterData(self._items[0]).count

        self.band_indexes = range(1, band_count + 1) \
            if indexes is None else indexes

        assert not min(self.band_indexes) < 1, \
            IndexError("Bands are indexed from 1")

        assert not max(self.band_indexes) > band_count, \
            IndexError("Band index out of range")

    def __iter__(self):
        for i in self._items:
            yield RasterData(i, indexes=self.band_indexes)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, args):
        try:
            key, bands = args[0], args[1]
        except (KeyError, TypeError, IndexError):
            key, bands = args, None

        if isinstance(bands, int):
            bands = [bands]

        if isinstance(key, slice):
            idx = key.indices(len(self._items))
            return RasterDataCollection(
                [self._items[i] for i in range(*idx)],
                indexes=self.band_indexes if bands is None else bands,
                verify=False
            )
        elif isinstance(key, int):
            return RasterData(
                self._items[key],
                indexes=self.band_indexes if bands is None else bands
            )
        else:
            raise IndexError("{} must be of type slice, or int")

    @property
    def shape(self):
        # NOTE: Assumes all datasets in collection have the same shape
        return self[0].shape

    @property
    def min(self):
        if len(self) == 1:
            return self[0].min
        else:
            return [rd.min for rd in self]

    @property
    def max(self):
        if len(self) == 1:
            return self[0].max
        else:
            return [rd.max for rd in self]

    @property
    def mean(self):
        if len(self) == 1:
            return self[0].mean
        else:
            return [rd.mean for rd in self]

    @property
    def stddev(self):
        if len(self) == 1:
            return self[0].stddev
        else:
            return [rd.stddev for rd in self]

    @property
    def nodata(self):
        # HACK: assume nodata is consistent across
        #       all RasterData/bands.
        return [rd.nodata for rd in self][0]

    def ix(self, *args, **kwargs):
        if len(self) == 1:
            return self[0].ix(*args, **kwargs)
        else:
            return np.ma.masked_values(
                [rd.ix(*args, **kwargs) for rd in self],
                self.__getitem__((0, 1)).nodata)

    def get_data(self, *args, **kwargs):
        masked = kwargs.get("masked", True)
        # TODO: fixed masked array hack here
        if masked:
            kwargs["masked"] = False
            return np.ma.masked_values(
                np.array([rd.get_data(*args, **kwargs) for rd in self]),
                self.__getitem__((0, 1)).nodata)
        else:
            return np.array([rd.get_data(*args, **kwargs) for rd in self])

    def get_names(self):
        return [rd.name for rd in self]

    def index(self, *args, **kwargs):
        # TODO: Fix this so it doesn't just assume
        #       index is consistent across timesteps
        return self.__getitem__(0).index(*args, **kwargs)
