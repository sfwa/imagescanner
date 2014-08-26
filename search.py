import os
import sys
import plog
import math
import gzip
import json
import base64
import cStringIO
import subprocess

WGS84_A = 6378137.0
FOV_X = 1.712
FOV_Y = FOV_X

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


def detect(pgm_path, out_path):
    lat = None
    lon = None
    alt = None
    q = None

    # Extract the attitude estimate for the current image
    fdir, fname = os.path.split(pgm_path)
    telem_path = os.path.join(
        fdir, "telem" + fname[len("img"):].rpartition(".")[0] + ".txt")
    with open(telem_path, "r") as telem_in:
        last_telem = list(p for p in plog.iterlogs_raw(telem_in))[-1]
        attitude_est = plog.ParameterLog.deserialize(last_telem)

        lat, lon, alt = attitude_est.find_by(
            device_id=0,
            parameter_type=plog.ParameterType.FCS_PARAMETER_ESTIMATED_POSITION_LLA).values
        lat = math.degrees(lat * math.pi / 2**31)
        lon = math.degrees(lon * math.pi / 2**31)
        alt *= 1e-2

        q = attitude_est.find_by(
            device_id=0,
            parameter_type=plog.ParameterType.FCS_PARAMETER_ESTIMATED_ATTITUDE_Q).values
        q = map(lambda x: float(x) / 32767.0, q)

    img_info = {
        "session": os.path.split(fdir)[1],
        "name": fname.rpartition(".")[0] + "_%.8f_%.8f_%.2f_%.8f_%.8f_%.8f_%.8f.jpg" % (lat, lon, alt, q[0], q[1], q[2], q[3]),
        "targets": [],
        "thumb": None
    }

    # Find blob coordinates
    with open(pgm_path, "r") as img_in:
        # Debayer the image
        debayer = subprocess.Popen(
            ["./debayer", "GBRG", os.path.join(out_path, img_info["name"])], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        img_out, blob_out = debayer.communicate(img_in.read())

        if img_out:
            img_info["thumb"] = base64.b64encode(img_out)

            for blob in blob_out.split("\n"):
                if not blob:
                    continue

                blob = eval(blob)
                geo = geo_from_cam(cam_from_img((blob[1], blob[2])), lat, lon, alt, q)
                img_info["targets"].append({"lat": geo[0], "lon": geo[1], "score": blob[3], "x": blob[1], "y": blob[2]})

    return json.dumps(img_info)

    out = cStringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
        f.write()
    return out.getvalue()

# TODO:
# - Start up Tornado, create a WebSocketClient, and connect to the gateway server
# - Read last image status
# - For each even-numbered unprocessed image in SRC_DIR, run detect; write the full image to OUT_DIR, and send the thumbnails and metadata to the gateway server via the web socket
# - For each unprocessed image in OUT_DIR, upload it to the gateway server

if __name__ == "__main__":
    for arg in sys.argv[2:]:
        try:
            print len(detect(arg, sys.argv[1]))
        except Exception:
            print "Couldn't process %s" % arg
            raise
