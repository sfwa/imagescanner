import os
import re
import sys
import plog
import math
import json
import base64
import serial
import socket
import collections
import tornado.web
import tornado.gen
import logging as log
import tornado.ioloop
import tornado.process
import tornado.iostream
import tornado.websocket

WS_URL = "ws://{0}:31285/P48WeGkwEFxheWVYEz6rGz4bbiwX4gkT"
WS_IMAGE = "http://{0}:31285/vQivxdjcFcUH34mLAEcfm77varwTmAA8/{1}/{2}"

DSP_CONN = None

NUM_PATTERN = re.compile('(\d+)\.')


def sort_numbered_files(files):
    files.sort(key=lambda x: int(NUM_PATTERN.search(x).group(1)))


def info_from_telemetry_file(telemetry_path, img_dir, img_name):
    lat = 0.0
    lon = 0.0
    alt = 0.0
    q = [0.0, 0.0, 0.0, 1.0]
    telem = None

    try:
        with open(telemetry_path, "r") as telem_file:
            frames = list(p for p in plog.iterlogs_raw(telem_file))
            if frames:
                # Use the earliest data in the frame
                target_frame = frames[1]
                log_data = plog.ParameterLog.deserialize(target_frame)

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

                # This is the latest data, which is what we'll send back to
                # the relay
                telem = frames[-1]
    except Exception:
        pass

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
        "q": list(q),
        "telemetry": base64.b64encode(telem)
    }


@tornado.gen.coroutine
def scan_image(pgm_path, dest_dir):
    log.info("scan_image({0}, {1})".format(repr(pgm_path), repr(dest_dir)))

    # Extract the attitude estimate for the current image
    pgm_dir, pgm_name = os.path.split(pgm_path)
    telemetry_path = os.path.join(
        pgm_dir, "telem{0}.txt".format(int(pgm_name[len("img"):].rpartition(".")[0])))
    img_info = info_from_telemetry_file(telemetry_path, pgm_dir, pgm_name)

    # Find blob coordinates
    with open(pgm_path, "r") as pgm_file:
        # Debayer the image
        debayer = tornado.process.Subprocess(
            [os.path.join(os.path.split(os.path.realpath(__file__))[0], "debayer"),
             "GBRG", os.path.join(pgm_dir, img_info["name"])],
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

        if thumb_out and img_info["alt"] > 20.0:
            img_info["thumb"] = base64.b64encode(thumb_out)

            for blob in blob_out.split("\n"):
                if not blob:
                    continue

                b = eval(blob)
                img_info["targets"].append({"x": b[1], "y": b[2]})

    # Move the image to the 'processed' directory
    os.rename(
        pgm_path,
        os.path.join(dest_dir, img_info["session"] + "-" + pgm_name))
    try:
        os.rename(
            telemetry_path,
            os.path.join(
                dest_dir,
                img_info["session"] + "-" + os.path.split(telemetry_path)[1]))
    except Exception:
        pass

    raise tornado.gen.Return(img_info)


class PatchedTCPClient(tornado.tcpclient.TCPClient):
    def _create_stream(self, max_buffer_size, af, addr):
        stream = tornado.iostream.IOStream(socket.socket(af),
                                           io_loop=self.io_loop,
                                           max_buffer_size=max_buffer_size)
        stream.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        log.debug("PatchedTCPClient._create_stream(...): set socket send "
                  "buffer size to 8 kB")
        return stream.connect(addr)


@tornado.gen.coroutine
def send_images(scan_dir, dest_dir, host):
    while True:
        try:
            next_file = None
            files = []
            for subdir in os.listdir(scan_dir):
                subdir_path = os.path.join(scan_dir, subdir)
                if os.path.isdir(subdir_path):
                    files += list(os.path.join(subdir_path, f)
                                  for f in os.listdir(subdir_path)
                                  if f.endswith(".jpg"))

            # Wait a second in between scans
            if not files:
                yield tornado.gen.Task(
                    tornado.ioloop.IOLoop.instance().call_later, 1.0)
                continue

            # Ignore all but the latest file -- move the rest to the processed
            # directory
            sort_numbered_files(files)
            next_file = files[0]
            fdir, name = os.path.split(next_file)

            # Only every 3rd image is processed, so this means every 6th is
            # uploaded
            if int(name.partition("_")[0][len("img"):]) % 2 == 0:
                with open(next_file, 'rb') as img_file:
                    img_data = img_file.read()

                session = os.path.split(fdir)[1].partition('-')[2]
                http_client = tornado.httpclient.AsyncHTTPClient()


                # Install a monkeypatch to set the HTTP client's socket send
                # buffer size
                if not isinstance(http_client.tcp_client, PatchedTCPClient):
                    http_client.tcp_client = PatchedTCPClient(
                        resolver=http_client.resolver,
                        io_loop=http_client.tcp_client.io_loop)

                # Rate-limit the uploads -- no more than 1 image every 2 seconds
                result = yield [
                    http_client.fetch(WS_IMAGE.format(host, session, name),
                                      method="POST", body=img_data),
                    tornado.gen.Task(tornado.ioloop.IOLoop.instance().call_later,
                                     2.0)
                ]

                log.info(
                    "send_images({0}, {1}): uploaded image {2}".format(
                    repr(scan_dir), repr(dest_dir), repr(next_file)))

            os.rename(next_file,
                      os.path.join(dest_dir, os.path.split(next_file)[1]))
        except Exception:
            log.exception(
                "send_images({0}, {1}): couldn't upload image {2}".format(
                repr(scan_dir), repr(dest_dir), repr(next_file)))


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

            # Ignore 2 out of 3 files -- move them straight to the processed
            # directory
            sort_numbered_files(files)
            scan_files = []
            for f in files:
                # Ignore JPEGs
                f_name, _, f_type = os.path.split(f)[1].rpartition(".")
                if f_type not in ("pgm", "txt"):
                    continue
                f_idx = int(f_name[len("img"):]
                            if f_type == "pgm" else f_name[len("telem"):])

                if f_idx % 3:
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

            # Run up to three concurrent scans
            if len(scan_files) > 2:
                log.info("scan_images(): opening three scan processes")
                img_infos = yield [scan_image(scan_files[0], dest_dir),
                                  scan_image(scan_files[1], dest_dir),
                                  scan_image(scan_files[2], dest_dir)]
            elif len(scan_files) == 2:
                log.info("scan_images(): opening two scan processes")
                img_infos = yield [scan_image(scan_files[0], dest_dir),
                                  scan_image(scan_files[1], dest_dir)]
            else:
                log.info("scan_images(): opening one scan process")
                img_infos = yield [scan_image(scan_files[0], dest_dir)]

            # Queue the info to be sent, assuming targets were found
            for img_info in img_infos:
                target_queue.append(img_info)
        except Exception:
            log.exception(
                "scan_images({0}, {1}): scan run encountered an error".format(
                repr(scan_dir), repr(dest_dir)))


@tornado.gen.coroutine
def send_targets(target_queue, host):
    global DSP_CONN

    log.info("poll_ws({0})".format(repr(host)))
    io_loop = tornado.ioloop.IOLoop.instance()

    last_dsp_msg = None

    while True:
        # Rate-limit re-connects
        yield tornado.gen.Task(
                tornado.ioloop.IOLoop.instance().call_later, 2.0)

        try:
            # Ensure the web socket is connected
            log.info("scan_images(): connecting web socket")
            ws = yield tornado.websocket.websocket_connect(
                WS_URL.format(host), connect_timeout=30)

            # After a disconnect, skip any telemetry-only messages so we don't
            # fall behind in target processing
            while len(target_queue):
                if len(target_queue[0]["targets"]):
                    break
                else:
                    target_queue.popleft()

            while ws.protocol:
                # Set nodelay on the connection's TCP socket
                ws.stream.set_nodelay(True)

                # Closing the socket will trigger a null message, which
                # will abort the coroutine
                timeout = io_loop.call_later(10.0, ws.close)

                if len(target_queue):
                    msg = json.dumps(target_queue.popleft())
                else:
                    msg = "{}"
                ws.write_message(msg)
                log.info("send_targets(): wrote message {0}".format(msg))

                # Minimum 0.5-second delay per loop iteration -- stop the
                # connection from being hit too heavily
                resp, _ = yield [
                    ws.read_message(),
                    tornado.gen.Task(
                        tornado.ioloop.IOLoop.instance().call_later, 0.5)
                ]
                if resp is None:
                    log.info("send_targets(): socket was closed")
                    ws.close()  # For good measure
                    # Put the message back at the head of the queue
                    if msg != "{}":
                        target_queue.appendleft(json.loads(msg))
                    break
                elif resp[0] == "\x00" and resp[-1] == "\x00" and \
                        DSP_CONN and resp != last_dsp_msg:
                    log.debug("send_targets(): forwarded message to DSP")
                    # Looks like a telemetry packet, send it to the DSP
                    DSP_CONN.write(resp)
                    last_dsp_msg = resp

                log.info(
                    "send_targets(): read message {0}".format(repr(resp)))
                io_loop.remove_timeout(timeout)

            ws.close()  # Just in case
        except Exception:
            log.exception("send_targets(): send run encountered an error")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        log.error("Usage: scanner.py SCAN_DIR DEST_DIR HOST [DSP_CONN]")
        sys.exit(1)

    scan_dir = sys.argv[1]
    dest_dir = sys.argv[2]
    host = sys.argv[3]
    if len(sys.argv) > 4:
        DSP_CONN = serial.Serial(port=sys.argv[4], baudrate=115200)

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


