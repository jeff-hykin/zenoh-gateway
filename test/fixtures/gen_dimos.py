"""Stage 1 (dimos venv): encode with dimos's own msg classes, round-trip with lcm_decode, write dimos/*.lcm."""

import io
import json
from pathlib import Path
import sys

import numpy as np
from PIL import Image as PILImage

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common as c  # noqa: E402

from dimos.msgs.sensor_msgs.CompressedImage import CompressedImage  # noqa: E402
from dimos.msgs.sensor_msgs.Image import Image, ImageFormat  # noqa: E402
from dimos.msgs.sensor_msgs.PointCloud2 import PointCloud2  # noqa: E402

OUT = HERE / "dimos"
COMPRESSED_OUT = HERE / "compressed"
ENCODING_TO_FORMAT = {
    "rgb8": ImageFormat.RGB,
    "bgr8": ImageFormat.BGR,
    "mono8": ImageFormat.GRAY,
    "mono16": ImageFormat.GRAY16,
    "16UC1": ImageFormat.DEPTH16,
    "32FC1": ImageFormat.DEPTH,
}
entries = []


def key(topic, cls):
    return f"dimos/{topic}/{cls.msg_name}"


def write(name, payload, **meta):
    path = OUT / f"{name}.lcm"
    path.write_bytes(payload)
    entries.append({"file": f"dimos/{path.name}", "protocol": "dimos-lcm", "bytes": len(payload), **meta})


def assert_header(msg_obj, frame_id):
    assert msg_obj.frame_id == frame_id, msg_obj.frame_id
    assert abs(msg_obj.ts - c.TS) < 1e-6, msg_obj.ts


# --- raw Images ---
for name, arr, encoding, frame_id, topic in c.raw_image_cases():
    img = Image(data=arr, format=ENCODING_TO_FORMAT[encoding], frame_id=frame_id, ts=c.TS)
    payload = img.lcm_encode()
    back = Image.lcm_decode(payload)
    assert back.data.dtype == arr.dtype and np.array_equal(back.data, arr), name
    assert back.format == img.format, (name, back.format)
    assert_header(back, frame_id)
    channels = 1 if arr.ndim == 2 else arr.shape[2]
    write(
        name,
        payload,
        format="raw",
        msg_type=Image.msg_name,
        zenoh_key=key(topic, Image),
        frame_id=frame_id,
        stamp={"sec": 1700000000, "nanosec": 500000000},
        width=c.WIDTH,
        height=c.HEIGHT,
        encoding=encoding,
        step=c.WIDTH * arr.dtype.itemsize * channels,
        is_bigendian=False,
        expected=c.expected_for_raw(arr, encoding),
    )

# --- dimos-specific: JPEG bytes inside a sensor_msgs.Image (encoding="jpeg", step=0) ---
rgb_img = Image(data=c.rgb_pattern(), format=ImageFormat.RGB, frame_id=c.COLOR_FRAME, ts=c.TS)
payload = rgb_img.lcm_jpeg_encode(quality=90)
back = Image.lcm_decode(payload)
assert back.format == ImageFormat.RGB and back.data.shape == (c.HEIGHT, c.WIDTH, 3)
assert_header(back, c.COLOR_FRAME)
means = c.quadrant_means(back.data)
for q, want in c.QUADRANT_RGB.items():
    assert max(abs(a - b) for a, b in zip(means[q], want)) < 8, (q, means[q])
write(
    "image_jpeg_in_Image",
    payload,
    format="jpeg",
    msg_type=Image.msg_name,
    zenoh_key=key("camera/color", Image),
    frame_id=c.COLOR_FRAME,
    width=c.WIDTH,
    height=c.HEIGHT,
    encoding="jpeg",
    step=0,
    note="dimos Image.lcm_jpeg_encode: sensor_msgs.Image whose data is a JPEG file and encoding='jpeg'",
    expected={
        "lossless": False,
        "tolerance_per_channel": 8,
        "nominal_quadrant_rgb": {k: list(v) for k, v in c.QUADRANT_RGB.items()},
        "decoded_quadrant_mean_rgb": means,
        "decoded_mean_rgb": c.overall_mean_rgb(back.data),
    },
)

# --- CompressedImage ---
COMPRESSED_OUT.mkdir(exist_ok=True)


def webp_bytes(rgb):
    buf = io.BytesIO()
    PILImage.fromarray(rgb, "RGB").save(buf, format="WEBP", lossless=True)
    return buf.getvalue()


compressed_cases = [
    # name, CompressedImage, lossless, tolerance, encoder
    ("compressed_jpeg", CompressedImage.from_image(rgb_img, "jpeg", quality=90), False, 8, "dimos CompressedImage.from_image (turbojpeg q90)"),
    ("compressed_png", CompressedImage.from_image(rgb_img, "png"), True, 0, "dimos CompressedImage.from_image (cv2)"),
    ("compressed_jxl", CompressedImage.from_image(rgb_img, "jxl", quality=90), False, 8, "dimos CompressedImage.from_image (imagecodecs, lossy uint8)"),
    ("compressed_webp", CompressedImage(data=webp_bytes(c.rgb_pattern()), format="webp", frame_id=c.COLOR_FRAME, ts=c.TS), True, 0, "Pillow lossless WEBP (dimos has no webp encoder)"),
]
depth_img = Image(data=c.depth16_pattern(), format=ImageFormat.DEPTH16, frame_id=c.DEPTH_FRAME, ts=c.TS)
depth_jxl = CompressedImage.from_image(depth_img, "jxl")

for name, comp, lossless, tol, encoder in compressed_cases:
    payload = comp.lcm_encode()
    back = CompressedImage.lcm_decode(payload)
    assert back.data == comp.data and back.format == comp.format, name
    assert_header(back, c.COLOR_FRAME)
    if comp.format == "webp":
        decoded = np.asarray(PILImage.open(io.BytesIO(back.data)).convert("RGB"))
    else:
        decoded = back.decode().to_rgb().data
    means = c.quadrant_means(decoded)
    for q, want in c.QUADRANT_RGB.items():
        assert max(abs(a - b) for a, b in zip(means[q], want)) <= tol, (name, q, means[q])
    if lossless:
        assert np.array_equal(decoded, c.rgb_pattern()), name
    ext = comp.format
    (COMPRESSED_OUT / f"pattern.{ext}").write_bytes(comp.data)
    write(
        name,
        payload,
        format=comp.format,
        msg_type=CompressedImage.msg_name,
        zenoh_key=key("camera/color/compressed", CompressedImage),
        frame_id=c.COLOR_FRAME,
        width=c.WIDTH,
        height=c.HEIGHT,
        encoder=encoder,
        compressed_bytes_file=f"compressed/pattern.{ext}",
        expected={
            "lossless": lossless,
            "tolerance_per_channel": tol,
            "nominal_quadrant_rgb": {k: list(v) for k, v in c.QUADRANT_RGB.items()},
            "decoded_quadrant_mean_rgb": means,
            "decoded_mean_rgb": c.overall_mean_rgb(decoded),
        },
    )

payload = depth_jxl.lcm_encode()
back = CompressedImage.lcm_decode(payload)
decoded = back.decode()
assert decoded.data.dtype == np.uint16 and np.array_equal(decoded.data, c.depth16_pattern())
(COMPRESSED_OUT / "depth16.jxl").write_bytes(depth_jxl.data)
write(
    "compressed_jxl_depth16",
    payload,
    format="jxl",
    msg_type=CompressedImage.msg_name,
    zenoh_key=key("camera/depth/compressed", CompressedImage),
    frame_id=c.DEPTH_FRAME,
    width=c.WIDTH,
    height=c.HEIGHT,
    encoder="dimos CompressedImage.from_image (imagecodecs, lossless uint16)",
    compressed_bytes_file="compressed/depth16.jxl",
    note="decodes to a single-channel uint16 image (16UC1 depth, mm)",
    expected={"lossless": True, **c.expected_for_raw(c.depth16_pattern(), "16UC1")},
)

# --- PointCloud2 ---
xyz, intensity = c.point_cloud()
pc_cases = [
    ("pointcloud_xyzi", PointCloud2.from_numpy(xyz, frame_id=c.LIDAR_FRAME, timestamp=c.TS, intensities=intensity), intensity),
    ("pointcloud_xyz", PointCloud2.from_numpy(xyz, frame_id=c.LIDAR_FRAME, timestamp=c.TS), None),
]
for name, pc, inten in pc_cases:
    payload = pc.lcm_encode()
    back = PointCloud2.lcm_decode(payload)
    assert np.array_equal(back.points_f32(), xyz), name
    got_i = back.intensities_f32()
    want_i = inten if inten is not None else np.zeros(c.N_POINTS, dtype=np.float32)
    if got_i is not None:
        assert np.array_equal(got_i, want_i), name
    else:
        assert inten is None, name
    assert_header(back, c.LIDAR_FRAME)
    write(
        name,
        payload,
        format="pointcloud",
        msg_type=PointCloud2.msg_name,
        zenoh_key=key("lidar/points", PointCloud2),
        frame_id=c.LIDAR_FRAME,
        point_count=c.N_POINTS,
        height=1,
        width=c.N_POINTS,
        point_step=16,
        fields=[{"name": n, "offset": 4 * k, "datatype": 7, "count": 1} for k, n in enumerate(["x", "y", "z", "intensity"])],
        note=(
            None
            if inten is not None
            else "dimos PointCloud2.lcm_encode always emits x,y,z,intensity (point_step 16); an xyz-only cloud carries intensity=0"
        ),
        expected={
            "first_points": c.first_points(xyz, want_i),
            "formula": "x=(i%200)*0.05, y=(i//200)*0.05, z=(i%7)*0.125, intensity=i%256" + ("" if inten is not None else " (here 0)"),
            "min_xyz": [float(v) for v in xyz.min(axis=0)],
            "max_xyz": [float(v) for v in xyz.max(axis=0)],
        },
    )

for e in entries:
    if e.get("note") is None:
        e.pop("note", None)
(HERE / "manifest.dimos.json").write_text(json.dumps(entries, indent=2))
print(f"dimos: wrote and round-tripped {len(entries)} payloads")
