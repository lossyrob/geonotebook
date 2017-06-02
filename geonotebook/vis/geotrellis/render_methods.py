from PIL import Image
import numpy as np

def hex_to_rgb(value):
    """Return (red, green, blue) for the color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

nlcd_color_map =  { 0: "#00000000",
                    11 : "#526095FF",     # Open Water
                    12 : "#FFFFFFFF",     # Perennial Ice/Snow
                    21 : "#D28170FF",     # Low Intensity Residential
                    22 : "#EE0006FF",     # High Intensity Residential
                    23 : "#990009FF",     # Commercial/Industrial/Transportation
                    31 : "#BFB8B1FF",     # Bare Rock/Sand/Clay
                    32 : "#969798FF",     # Quarries/Strip Mines/Gravel Pits
                    33 : "#382959FF",     # Transitional
                    41 : "#579D57FF",     # Deciduous Forest
                    42 : "#2A6B3DFF",     # Evergreen Forest
                    43 : "#A6BF7BFF",     # Mixed Forest
                    51 : "#BAA65CFF",     # Shrubland
                    61 : "#45511FFF",     # Orchards/Vineyards/Other
                    71 : "#D0CFAAFF",     # Grasslands/Herbaceous
                    81 : "#CCC82FFF",     # Pasture/Hay
                    82 : "#9D5D1DFF",     # Row Crops
                    83 : "#CD9747FF",     # Small Grains
                    84 : "#A7AB9FFF",     # Fallow
                    85 : "#E68A2AFF",     # Urban/Recreational Grasses
                    91 : "#B6D8F5FF",     # Woody Wetlands
                    92 : "#B6D8F5FF" }    # Emergent Herbaceous Wetlands

def nlcd_color_palette():
    palette = [0, 0, 0] * 100
    for key in nlcd_color_map:
        i = key * 3
        (r, g, b, _) = hex_to_rgb(nlcd_color_map[key])
        palette[i] = r
        palette[i + 1] = g
        palette[i + 2] = b
    return palette

def rgba_functions(color_map):
    m = {}
    for key in color_map:
        m[key] = hex_to_rgb(color_map[key])

    def r(v):
        if v in m:
            return m[v][0]
        else:
            return 0

    def g(v):
        if v in m:
            return m[v][1]
        else:
            return 0

    def b(v):
        if v in m:
            return m[v][2]
        else:
            return 0

    def a(v):
        if v in m:
            return m[v][3]
        else:
            return 0x00

    return (np.vectorize(r), np.vectorize(g), np.vectorize(b), np.vectorize(a))

def render_nlcd(tile):
    '''
    Assumes that the tile is a multiband tile with a single band.
    (meaning shape = (1, cols, rows))
    '''
    arr = tile[0]
    (r, g, b, a) = rgba_functions(nlcd_color_map)

    rgba = np.dstack([r(arr), g(arr), b(arr), a(arr)]).astype('uint8')

    img = Image.fromarray(rgba, mode='RGBA')

    return img

def single_band_render_from_color_map(color_map):
    def render(tile):
        (r, g, b, a) = rgba_functions(color_map)
        rgba = np.dstack([r(tile), g(tile), b(tile), a(tile)]).astype('uint8')
        return Image.fromarray(rgba, mode='RGBA')
    return render

def render_default_rdd(tile):
    def make_image(arr):
        return Image.fromarray(arr.astype('uint8')).convert('L')

    def clamp(x):
        if (x < 0.0):
            x = 0
        elif (x >= 1.0):
            x = 255
        else:
            x = (int)(x * 255)
        return x

    def alpha(x):
        if ((x <= 0.0) or (x > 1.0)):
            return 0
        else:
            return 255

    clamp = np.vectorize(clamp)
    alpha = np.vectorize(alpha)


    bands = tile.shape[0]
    if bands >= 3:
        bands = 3
    else:
        bands = 1
    arrs = [np.array(tile[i, :, :]).reshape(256, 256) for i in range(bands)]

    # create tile
    if bands == 3:
        images = [make_image(clamp(arr)) for arr in arrs]
        images.append(make_image(alpha(arrs[0])))
        return Image.merge('RGBA', images)
    else:
        gray = make_image(clamp(arrs[0]))
        alfa = make_image(alpha(arrs[0]))
        return Image.merge('RGBA', list(gray, gray, gray, alfa))
