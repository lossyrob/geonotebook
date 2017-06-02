import io, os
import logging
import math
import numpy as np
import os
import rasterio
import threading
import sys
import time
import traceback

from gevent.pywsgi import WSGIServer

###
from PIL import Image, ImageDraw, ImageFont

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
###

from flask import Flask, make_response, abort, request

def respond_with_image(image):
    bio = io.BytesIO()
    image.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response

def make_tile_server(port, fn):
    '''
    Makes a tile server and starts it on the given port, using a function
    that takes z, x, y as the tile route.
    '''
    app = Flask(__name__)
    app.logger.disabled = True
    logging.getLogger('werkzeug').disabled = True
    http_server = WSGIServer(('', port), app)

    f = open(os.devnull, "w")
    # sys.stdout = f
    sys.stderr = f

    def shutdown():
        time.sleep(0.5)
        http_server.stop()

    @app.route('/shutdown')
    def shutdown():
        try:
            t = threading.Thread(target=shutdown)
            t.start()
            # Do not return a response, as this causes odd issues.
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    @app.route("/tile/<int:z>/<int:x>/<int:y>.png")
    def tile(z, x, y):
        try:
            return fn(z, x, y)
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    return http_server.serve_forever()

def rdd_server(port, pyramid, render_tile):
    def tile(z, x, y):

        # fetch data
        rdd = pyramid[z]
        tile = rdd.lookup(col=x, row=y)

        arr = tile[0]['data']

        if arr == None:
            abort(404)

        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def catalog_layer_server(port,
                         value_reader,
                         layer_names,
                         is_multi_layer,
                         key_type,
                         tile_type,
                         avroregistry,
                         max_zoom,
                         render_tile):
    from geopyspark.avroserializer import AvroSerializer

    decoder = avroregistry._get_decoder(tile_type)
    encoder = avroregistry._get_encoder(tile_type)

    def tile(z, x, y):
        if z > max_zoom:
            overzoom = True
            dz = z - max_zoom
            tz = max_zoom
            tx = math.floor(x / math.pow(2, dz))
            ty = math.floor(y / math.pow(2, dz))
        else:
            overzoom = False
            tz = z
            tx = x
            ty = y

        tiles = []
        for layer_name in layer_names:
            value = value_reader.readTile(key_type,
                                          layer_name,
                                          tz,
                                          tx,
                                          ty,
                                          "")
            if not value:
                abort(404)

            ser = AvroSerializer(value._2(), decoder, encoder)
            tile = ser.loads(value._1())[0]['data']

            tiles.append(tile)

        if is_multi_layer:
            image = render_tile(tiles)
        else:
            image = render_tile(tiles[0])

        if overzoom:
            # Figure out image crop bounds
            dz = z - max_zoom
            dx = x - tx * math.pow(2, dz)
            dy = y - ty * math.pow(2, dz)

            (w, h) = image.size

            tw = int(w / dz)
            th = int(h / dz)
            (x0, x1) = (tw * dx, tw * (dx + 1))
            (y0, y1) = (th * dy, th * (dy + 1))
            image = image.crop(x0, y0, x1, y1)
            image = image.resample((256,256))

        return respond_with_image(image)

    return make_tile_server(port, tile)

def png_layer_server(port, png):
    def tile(z, x, y):

        # fetch data
        try:
            img = png.lookup(x, y, z)
        except:
            img = None

        if img == None or len(img) == 0:
            if png.debug:
                image = Image.new('RGBA', (256,256))
                draw = ImageDraw.Draw(image)
                draw.rectangle([0, 0, 255, 255], outline=(255,0,0,255))
                draw.line([(0,0),(255,255)], fill=(255,0,0,255))
                draw.line([(0,255),(255,0)], fill=(255,0,0,255))
                draw.text((136,122), str(x) + ', ' + str(y) + ', ' + str(zoom), fill=(255,0,0,255))
                del draw
                bio = io.BytesIO()
                image.save(bio, 'PNG')
                img = [bio.getvalue()]
            else:
                abort(404)

        response = make_response(img[0])
        response.headers['Content-Type'] = 'image/png'
        return response

    return make_tile_server(port, tile)
