var SphericalMercator = require('sphericalmercator');
var merc = new SphericalMercator();

/**
 * Calculate an ideal zoom range based on data size and geographic extent.
 * Makes an implicit assumption that the data is evenly distributed geographically
 * - max = zoom level at which a single tile would contain < 1 kilobyte
 * - min = zoom level at which a single tile would contain > 500 kilobytes
 * - never sets max zoom > 22
 *
 * @private
 * @param {number} bytes - the number of bytes
 * @param {Array<number>} extent - the geographic extent in decimal degrees as [west, south, east, north]
 * @returns {object} an object with `min` and `max` properties corresponding to an ideal min and max zoom
 */
module.exports = function zoomRange(bytes, extent) {
    var maxSize = 500 * 1024;
    var maxzoom = 14;
    for (var z = 22; z >= 0; z--) {
        var bounds = merc.xyz(extent, z, false, 4326);
        var x = (bounds.maxX - bounds.minX) + 1;
        var y = (bounds.maxY - bounds.minY) + 1;
        var tiles = x * y;
        var avgTileSize = bytes / tiles;

        // The idea is that tilesize of ~1000 bytes is usually the most detail
        // needed, and no need to process tiles with higher zoom
        if (avgTileSize < 1000) maxzoom = z;

        // Tiles are getting too large at current z
        if (avgTileSize > maxSize) return { min: z, max: maxzoom };

        // If all the data fits into one tile, it'll fit all the way to z0
        if (tiles === 1 || z === 0) return { min: 0, max: maxzoom };
    }
}

