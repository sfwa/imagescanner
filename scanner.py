import os
import sys
import plog
import math
import json
import base64
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

WS_URL = "ws://{0}:31285/P48WeGkwEFxheWVYEz6rGz4bbiwX4gkT"
WS_IMAGE = "http://{0}:31285/vQivxdjcFcUH34mLAEcfm77varwTmAA8/{1}/{2}"


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

    # Transform body frame to world frame
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

    log.info("geo_from_cam: rx={0} ry={1} rz={2} fac={3} n={4} e={5}".format(rx, ry, rz, fac, n, e))

    # Convert north/east offsets to lat/lon
    return (
        v_lat + math.degrees(n / WGS84_A),
        v_lon + math.degrees(e / (WGS84_A * math.cos(math.radians(v_lat)))),
    )


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
    os.rename(
        pgm_path,
        os.path.join(dest_dir, img_info["session"] + "-" + pgm_name))
    os.rename(
        telemetry_path,
        os.path.join(
            dest_dir,
            img_info["session"] + "-" + os.path.split(telemetry_path)[1]))

    raise tornado.gen.Return(img_info)


@tornado.gen.coroutine
def send_images(scan_dir, dest_dir, host):
    while True:
        try:
            last_file = None
            files = []
            for subdir in os.listdir(scan_dir):
                subdir_path = os.path.join(scan_dir, subdir)
                if os.path.isdir(subdir_path):
                    files += list(os.path.join(subdir_path, f)
                                  for f in os.listdir(subdir_path)
                                  if f.endswith(".jpg"))

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
            result = yield http_client.fetch(
                WS_IMAGE.format(host, session, name),
                method="POST", body=img_data)

            os.rename(last_file,
                      os.path.join(dest_dir, os.path.split(last_file)[1]))
        except Exception:
            log.exception(
                "send_images({0}, {1}): couldn't upload image {2}".format(
                repr(scan_dir), repr(dest_dir), repr(last_file)))


@tornado.gen.coroutine
def scan_images(scan_dir, dest_dir, target_queue):
    while True:
        try:
            files = []
            for subdir in os.listdir(scan_dir):
                subdir_path = os.path.join(scan_dir, subdir)
                if os.path.isdir(subdir_path):
                    files += list(os.path.join(subdir_path, f)
                                  for f in os.listdir(subdir_path))

            # Ignore odd-numbered files -- move them straight to the processed
            # directory
            files.sort()
            scan_files = []
            for f in files:
                # Ignore JPEGs
                f_name, _, f_type = os.path.split(f)[1].rpartition(".")
                if f_type not in ("pgm", "txt"):
                    continue
                f_idx = int(f_name[len("img"):]
                            if f_type == "pgm" else f_name[len("telem"):])

                if f_idx % 2 == 1:
                    subdir = os.path.split(os.path.split(f)[0])[1]
                    os.rename(f, os.path.join(
                                    dest_dir,
                                    subdir.partition('-')[2] + "-" +
                                        os.path.split(f)[1]))
                elif f_type == "pgm":
                    scan_files.append(f)

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

            # Queue the info to be sent, assuming targets were found
            for img_info in img_infos:
                if img_info["targets"]:
                    target_queue.append(img_info)
        except Exception:
            log.exception(
                "scan_images({0}, {1}): scan run encountered an error".format(
                repr(scan_dir), repr(dest_dir)))


@tornado.gen.coroutine
def send_targets(target_queue, host):
    log.info("poll_ws({0})".format(repr(host)))
    io_loop = tornado.ioloop.IOLoop.instance()

    while True:
        try:
            # Ensure the web socket is connected
            log.info("scan_images(): connecting web socket")
            ws = yield tornado.websocket.websocket_connect(
                WS_URL.format(host), connect_timeout=30)

            while ws.protocol:
                while len(target_queue):
                    # Closing the socket will trigger a null message, which
                    # will abort the coroutine
                    timeout = io_loop.call_later(10.0, ws.close)

                    msg = json.dumps(target_queue.popleft())
                    ws.write_message(msg)
                    log.info("send_targets(): wrote message {0}".format(msg))

                    msg = yield ws.read_message()
                    if msg is None:
                        log.info("send_targets(): socket was closed")
                        ws.close()  # For good measure
                        # Put the message back at the head of the queue
                        target_queue.appendleft(json.loads(msg))
                        break

                    log.info(
                        "send_targets(): read message {0}".format(repr(msg)))
                    io_loop.remove_timeout(timeout)

                log.info("send_targets(): no targets to send")
                yield tornado.gen.Task(
                    tornado.ioloop.IOLoop.instance().call_later, 1.0)

            ws.close()  # Just in case
        except Exception:
            log.exception("send_targets(): send run encountered an error")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        log.error("Usage: scanner.py SCAN_DIR DEST_DIR HOST")
        sys.exit(1)

    scan_dir = sys.argv[1]
    dest_dir = sys.argv[2]
    host = sys.argv[3]

    if not os.path.exists(scan_dir):
        log.error("SCAN_DIR does not exist")
        sys.exit(1)

    if not os.path.exists(dest_dir):
        log.error("DEST_DIR does not exist")
        sys.exit(1)

    log.getLogger().setLevel(log.DEBUG)

    target_queue = collections.deque()

    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.add_callback(send_images, scan_dir, dest_dir, host)
    io_loop.add_callback(scan_images, scan_dir, dest_dir, target_queue)
    io_loop.add_callback(send_targets, target_queue, host)
    io_loop.start()


