from typing import Dict, List, Callable
import pickle
import httpx
from rtree import index
import attr
from morecantile import TileMatrixSet
from rio_tiler.io import BaseReader

from cogeo_mosaic.backends.base import BaseBackend
from cogeo_mosaic.errors import _HTTP_EXCEPTIONS, MosaicError
from cogeo_mosaic.mosaic import MosaicJSON


@attr.s
class DynamicRtreeBackend(BaseBackend):

    asset_filter: Callable = attr.ib(default=lambda x: x)
        
    # The reader is read-only, we can't pass mosaic_def to the init method
    mosaic_def: MosaicJSON = attr.ib(init=False)
    
    index = attr.ib(init=False)
    
    minzoom: int = attr.ib(init=False, default=12)  # we know this by analysing the NAIP data 
    maxzoom: int = attr.ib(init=False, default=17)  # we know this by analysing the NAIP data 

    _backend_name = "DynamicSTAC"

    def __attrs_post_init__(self):
        """Post Init."""
        # Construct a FAKE mosaicJSON
        # mosaic_def has to be defined. As we do for the DynamoDB and SQLite backend
        # we set `tiles` to an empty list.
        self.mosaic_def = MosaicJSON(
            mosaicjson="0.0.2",
            name="it's fake but it's ok",
            minzoom=self.minzoom,
            maxzoom=self.maxzoom,
            tiles=[]
        )

        try:
            if not self.input.endswith('.p'):
                self.input = self.input + '.p'
            r = httpx.get(self.input)
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # post-flight errors
            status_code = e.response.status_code
            exc = _HTTP_EXCEPTIONS.get(status_code, MosaicError)
            raise exc(e.response.content) from e
        except httpx.RequestError as e:
            # pre-flight errors
            raise MosaicError(e.args[0].reason) from e

        stream = pickle.loads(r.content)

        self.index = index.Index(stream)
        self.bounds = tuple(self.index.bounds)

    def close(self):
        """Close SQLite connection."""
        self.index.close()

    def __exit__(self, exc_type, exc_value, traceback):
        """Support using with Context Managers."""
        self.close()        
        
    def write(self, overwrite: bool = True):
        """This method is not used but is required by the abstract class."""
        pass

    def update(self):
        """We overwrite the default method."""
        pass

    def _read(self) -> MosaicJSON:
        """This method is not used but is required by the abstract class."""
        pass

    def assets_for_tile(self, x: int, y: int, z: int) -> List[str]:
        """Retrieve assets for tile."""
        bbox = self.tms.bounds(x, y, z)
        return self.get_assets(bbox)

    def assets_for_point(self, lng: float, lat: float) -> List[str]:
        """Retrieve assets for point."""
        EPSILON = 1e-14
        bbox = (lng - EPSILON, lat - EPSILON, lng + EPSILON, lat + EPSILON)
        return self.get_assets(bbox)

    def get_assets(self, bbox) -> List[str]:
        """Find assets."""
        assets = [n.object for n in self.index.intersection(bbox, objects=True)]
        return self.asset_filter(assets)

    @property
    def _quadkeys(self) -> List[str]:
        return []
