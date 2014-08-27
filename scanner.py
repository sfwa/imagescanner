import os
import sys
import plog
import math
import gzip
import json
import base64
import cStringIO
import subprocess
import collections
import tornado.web
import tornado.gen
import logging as log
import tornado.ioloop
import tornado.process
import tornado.websocket


WGS84_A = 6378137.0
FOV_X = 1.712
FOV_Y = FOV_X


WS_CONNECTION = None
WS_TIMEOUT = None
UPLOAD_RUNNING = False


def cam_from_img(img):
    # Convert x, y pixel coordinates to a point on a plane at one metre
    # from the camera
    pt_x = (img[0] - 640.0) / 640.0 * FOV_X
    pt_y = (img[1] - 480.0) / 480.0 * FOV_Y

    return (pt_x, pt_y)


def geo_from_cam(cam, v_lat, v_lon, v_alt, v_q):
    # Convert x, y image coordinates (in metres at 1.0m viewing distance) to
    # body-frame coordinates
    cx = -cam[1]
    cy = cam[0]
    cz = 1.0

    # Transform body frame to world frame (TODO: may need to take conjugate)
    qx, qy, qz, qw = v_q
    qx = -qx
    qy = -qy
    qz = -qz

    tx = 2.0 * (qy * cz - qz * cy)
    ty = 2.0 * (qz * cx - qx * cz)
    tz = 2.0 * (qx * cy - qy * cx)

    rx = cx + qw * tx + qy * tz - qz * ty
    ry = cy + qw * ty + qz * tx - qx * tz
    rz = cz + qw * tz - qy * tx + qx * ty

    # Project the ray down to where it intersects with the ground plane.
    # If the z-component is negative or zero, the point is in the sky.
    if rz < 0.001:
        return None

    fac = v_alt / rz
    n = rx * fac
    e = ry * fac

    # Convert north/east offsets to lat/lon
    return (
        v_lat + n / WGS84_A,
        v_lon + e / (WGS84_A * math.cos(v_lat)),
    )


def array_from_pgm(data, byteorder='>'):
    header, _, image = data.partition("\n")
    width, height, maxval = [int(item) for item in header.split()[1:]]
    return numpy.fromstring(
                image,
                dtype='u1' if maxval < 256 else byteorder + 'u2'
            ).reshape((height, width, 3))


def info_from_telemetry_file(telemetry_path, img_dir, img_name):
    with open(telemetry_path, "r") as telem_file:
        last_frame = list(p for p in plog.iterlogs_raw(telem_file))[-1]
        log_data = plog.ParameterLog.deserialize(last_frame)

        lat, lon, alt = log_data.find_by(
            device_id=0,
            parameter_type=plog.ParameterType.FCS_PARAMETER_ESTIMATED_POSITION_LLA).values
        lat = math.degrees(lat * math.pi / 2**31)
        lon = math.degrees(lon * math.pi / 2**31)
        alt *= 1e-2

        q = log_data.find_by(
            device_id=0,
            parameter_type=plog.ParameterType.FCS_PARAMETER_ESTIMATED_ATTITUDE_Q).values
        q = map(lambda x: float(x) / 32767.0, q)

    return {
        "session": os.path.split(img_dir)[1].partition('-')[2],
        "name": img_name.rpartition(".")[0] +
                "_%.8f_%.8f_%.2f_%.8f_%.8f_%.8f_%.8f.jpg" %
                    (lat, lon, alt, q[0], q[1], q[2], q[3]),
        "targets": [],
        "thumb": None,
        "lat": lat,
        "lon": lon,
        "alt": alt,
        "q": list(q)
    }


@tornado.gen.coroutine
def scan_image(pgm_path, dest_dir):
    log.info("scan_image({0}, {1})".format(repr(pgm_path), repr(dest_dir)))

    # Extract the attitude estimate for the current image
    pgm_dir, pgm_name = os.path.split(pgm_path)
    telemetry_path = os.path.join(
        pgm_dir, "telem" + pgm_name[len("img"):].rpartition(".")[0] + ".txt")
    img_info = info_from_telemetry_file(telemetry_path, pgm_dir, pgm_name)

    # Find blob coordinates
    with open(pgm_path, "r") as pgm_file:
        # Debayer the image
        debayer = tornado.process.Subprocess(
            ["./debayer", "GBRG", os.path.join(pgm_dir, img_info["name"])],
            stdin=tornado.process.Subprocess.STREAM,
            stdout=tornado.process.Subprocess.STREAM,
            stderr=tornado.process.Subprocess.STREAM, close_fds=True)

        # Careful -- if debayer ever tries to write to stdout before reading
        # all of stdin, this would be a race condition. Doesn't happen with
        # debayer's current architecture.
        yield debayer.stdin.write(pgm_file.read())
        debayer.stdin.close()

        thumb_out, blob_out = yield [debayer.stdout.read_until_close(),
                                     debayer.stderr.read_until_close()];

        if thumb_out:
            img_info["thumb"] = base64.b64encode(thumb_out)

            for blob in blob_out.split("\n"):
                if not blob:
                    continue

                blob = eval(blob)
                geo = geo_from_cam(cam_from_img((blob[1], blob[2])),
                                   img_info["lat"], img_info["lon"],
                                   img_info["alt"], img_info["q"])
                if not geo:
                    continue

                img_info["targets"].append({
                    "lat": geo[0], "lon": geo[1],
                    "score": blob[3],
                    "x": blob[1], "y": blob[2]
                })

    # Move the image to the 'processed' directory
    os.rename(pgm_path, os.path.join(dest_dir, pgm_name))
    os.rename(telemetry_path,
              os.path.join(dest_dir, os.path.split(telemetry_path)[1]))

    raise tornado.gen.Return(img_info)


@tornado.gen.coroutine
def send_images(scan_dir, dest_dir):
    while True:
        files = [os.path.join(scan_dir, f)
                 for f in os.listdir(scan_dir) if f.endswith(".jpg")]

        # Wait 0.5 seconds in between scans
        if not files:
            yield tornado.gen.Task(
                tornado.ioloop.IOLoop.instance().call_later, 0.5)
            continue

        # Ignore all but the latest file -- move the rest to the processed
        # directory
        files.sort()
        last_file = files.pop()
        for f in files:
            os.rename(f, os.path.join(dest_dir, os.path.split(f)[1]))

        with open(last_file, 'rb') as img_file:
            img_data = img_file.read()

        fdir, name = os.path.split(last_file)
        session = os.path.split(fdir)[1].partition('-')[2]

        http_client = tornado.httpclient.AsyncHTTPClient()
        try:
            result = yield http_client.fetch(
                "http://localhost:31285/vQivxdjcFcUH34mLAEcfm77varwTmAA8/{0}/{1}".format(session, name),
                method="POST",
                body=img_data)

            os.rename(last_file,
                      os.path.join(dest_dir, os.path.split(last_file)[1]))
        except Exception:
            log.exception(
                "send_images({0}, {1}): couldn't upload image {2}".format(
                repr(scan_dir), repr(dest_dir), repr(last_file)))


@tornado.gen.coroutine
def poll_ws(ws, *args):
    log.info("poll_ws()")
    io_loop = tornado.ioloop.IOLoop.instance()

    while True:
        # Closing the socket will trigger a null message, which will abort the
        # coroutine
        timeout = io_loop.call_later(10.0, ws.close)
        msg = yield ws.read_message()
        if msg is None:
            log.info("poll_ws(): socket was closed")
            return
        log.info("poll_ws(): read message {0}".format(repr(msg)))
        io_loop.remove_timeout(timeout)



@tornado.gen.coroutine
def scan_images(scan_dir, dest_dir):
    ws = None

    while True:
        files = [f for f in os.listdir(scan_dir)]

        # Ignore odd-numbered files -- move them straight to the processed
        # directory
        files.sort()
        scan_files = []
        for f in files:
            # Ignore JPEGs
            f_name, _, f_type = f.rpartition(".")
            if f_type not in ("pgm", "txt"):
                continue
            f_idx = int(f_name[len("img"):]
                        if f_type == "pgm" else f_name[len("telem"):])

            if f_idx % 2 == 1:
                os.rename(os.path.join(scan_dir, f),
                          os.path.join(dest_dir, os.path.split(f)[1]))
            elif f_type == "pgm":
                scan_files.append(os.path.join(scan_dir, f))

        # Wait one second in between scans
        if not scan_files:
            log.info("scan_images(): no files to scan")
            yield tornado.gen.Task(
                tornado.ioloop.IOLoop.instance().call_later, 1.0)
            continue

        # Run up to two concurrent scans
        if len(scan_files) > 1:
            log.info("scan_images(): opening two scan processes")
            img_infos = yield [scan_image(scan_files[0], dest_dir),
                              scan_image(scan_files[1], dest_dir)]
        else:
            log.info("scan_images(): opening one scan process")
            img_infos = yield [scan_image(scan_files[0], dest_dir)]

        # Ensure the web socket is connected
        if not ws or not ws.protocol:
            log.info("scan_images(): re-connecting web socket")
            ws = yield tornado.websocket.websocket_connect(
                "ws://localhost:31285/P48WeGkwEFxheWVYEz6rGz4bbiwX4gkT",
                connect_timeout=30)
            # Check for responses
            tornado.ioloop.IOLoop.instance().add_callback(poll_ws, ws)

        # Send the info
        for img_info in img_infos:
            msg = json.dumps(img_info)
            ws.write_message(msg)
            log.info("scan_images(): wrote message {0}".format(msg))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        log.error("Usage: scanner.py SCAN_DIR DEST_DIR")
        sys.exit(1)

    scan_dir = sys.argv[1]
    dest_dir = sys.argv[2]

    if not os.path.exists(scan_dir):
        log.error("SCAN_DIR does not exist")
        sys.exit(1)

    if not os.path.exists(dest_dir):
        log.error("DEST_DIR does not exist")
        sys.exit(1)

    log.getLogger().setLevel(log.DEBUG)

    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.add_callback(send_images, scan_dir, dest_dir)
    io_loop.add_callback(scan_images, scan_dir, dest_dir)
    io_loop.start()


