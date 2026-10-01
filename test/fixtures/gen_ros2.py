"""Stage 2 (rosbags env): serialize ROS 2 Jazzy CDR with rosbags, round-trip with deserialize_cdr, write ros2/*.cdr.

Reuses the compressed bytes stage 1 wrote to compressed/, so both protocols carry identical images.
"""

import io
import json
from pathlib import Path
import sys

import imagecodecs
import numpy as np
from PIL import Image as PILImage
from rosbags.typesys import Stores, get_typestore

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common as c  # noqa: E402

OUT = HERE / "ros2"
DOMAIN_ID = 0
ts = get_typestore(Stores.ROS2_JAZZY)
T = ts.types
Header = T["std_msgs/msg/Header"]
Time = T["builtin_interfaces/msg/Time"]
RosImage = T["sensor_msgs/msg/Image"]
RosCompressed = T["sensor_msgs/msg/CompressedImage"]
RosPointCloud2 = T["sensor_msgs/msg/PointCloud2"]
PointField = T["sensor_msgs/msg/PointField"]
entries = []
type_hashes = {}


def zenoh_key(topic, typename):
    pkg, _, name = typename.split("/")
    rihs = ts.hash_rihs01(typename)
    type_hashes[typename] = rihs
    return f"{DOMAIN_ID}/{topic}/{pkg}::msg::dds_::{name}_/{rihs}"


def header(frame_id):
    return Header(stamp=Time(sec=1700000000, nanosec=500000000), frame_id=frame_id)


def write(name, msg, typename, **meta):
    payload = ts.serialize_cdr(msg, typename)
    back = ts.deserialize_cdr(payload, typename)
    assert back.header.stamp.sec == 1700000000 and back.header.stamp.nanosec == 500000000
    assert back.header.frame_id == msg.header.frame_id
    path = OUT / f"{name}.cdr"
    path.write_bytes(payload)
    entries.append(
        {
            "file": f"ros2/{path.name}",
            "protocol": "ros2-cdr",
            "bytes": len(payload),
            "msg_type": typename,
            "cdr_encapsulation_header_hex": payload[:4].hex(),
            **meta,
        }
    )
    return back


# --- raw Images ---
for name, arr, encoding, frame_id, topic in c.raw_image_cases():
    channels = 1 if arr.ndim == 2 else arr.shape[2]
    step = c.WIDTH * arr.dtype.itemsize * channels
    msg = RosImage(
        header=header(frame_id),
        height=c.HEIGHT,
        width=c.WIDTH,
        encoding=encoding,
        is_bigendian=0,
        step=step,
        data=np.frombuffer(np.ascontiguousarray(arr).tobytes(), dtype=np.uint8),
    )
    ros_topic = topic + ("/image_rect_raw" if "depth" in topic else "/image_raw")
    back = write(
        name,
        msg,
        "sensor_msgs/msg/Image",
        format="raw",
        zenoh_key=zenoh_key(ros_topic, "sensor_msgs/msg/Image"),
        ros_topic="/" + ros_topic,
        frame_id=frame_id,
        stamp={"sec": 1700000000, "nanosec": 500000000},
        width=c.WIDTH,
        height=c.HEIGHT,
        encoding=encoding,
        step=step,
        is_bigendian=False,
        expected=c.expected_for_raw(arr, encoding),
    )
    assert (back.height, back.width, back.encoding, back.step) == (c.HEIGHT, c.WIDTH, encoding, step)
    decoded = np.frombuffer(back.data.tobytes(), dtype=arr.dtype).reshape(arr.shape)
    assert np.array_equal(decoded, arr), name


# --- CompressedImage (same bytes as the dimos fixtures) ---
def decode_rgb(fmt, data):
    if fmt == "jxl":
        return imagecodecs.jpegxl_decode(data)
    return np.asarray(PILImage.open(io.BytesIO(data)).convert("RGB"))


compressed_cases = [("jpeg", False, 8), ("png", True, 0), ("jxl", False, 8), ("webp", True, 0)]
for fmt, lossless, tol in compressed_cases:
    data = (HERE / "compressed" / f"pattern.{fmt}").read_bytes()
    msg = RosCompressed(header=header(c.COLOR_FRAME), format=fmt, data=np.frombuffer(data, dtype=np.uint8))
    back = write(
        f"compressed_{fmt}",
        msg,
        "sensor_msgs/msg/CompressedImage",
        format=fmt,
        zenoh_key=zenoh_key("camera/color/image_raw/compressed", "sensor_msgs/msg/CompressedImage"),
        ros_topic="/camera/color/image_raw/compressed",
        frame_id=c.COLOR_FRAME,
        width=c.WIDTH,
        height=c.HEIGHT,
        compressed_bytes_file=f"compressed/pattern.{fmt}",
        note="format string set the ROS way (image_transport uses e.g. 'rgb8; jpeg compressed bgr8'; here just the codec name)",
        expected=None,
    )
    assert back.format == fmt and back.data.tobytes() == data
    decoded = decode_rgb(fmt, back.data.tobytes())
    assert decoded.shape == (c.HEIGHT, c.WIDTH, 3), (fmt, decoded.shape)
    means = c.quadrant_means(decoded)
    for q, want in c.QUADRANT_RGB.items():
        assert max(abs(a - b) for a, b in zip(means[q], want)) <= tol, (fmt, q, means[q])
    if lossless:
        assert np.array_equal(decoded, c.rgb_pattern()), fmt
    entries[-1]["expected"] = {
        "lossless": lossless,
        "tolerance_per_channel": tol,
        "nominal_quadrant_rgb": {k: list(v) for k, v in c.QUADRANT_RGB.items()},
        "decoded_quadrant_mean_rgb": means,
        "decoded_mean_rgb": c.overall_mean_rgb(decoded),
    }

data = (HERE / "compressed" / "depth16.jxl").read_bytes()
back = write(
    "compressed_jxl_depth16",
    RosCompressed(header=header(c.DEPTH_FRAME), format="jxl", data=np.frombuffer(data, dtype=np.uint8)),
    "sensor_msgs/msg/CompressedImage",
    format="jxl",
    zenoh_key=zenoh_key("camera/depth/image_rect_raw/compressed", "sensor_msgs/msg/CompressedImage"),
    ros_topic="/camera/depth/image_rect_raw/compressed",
    frame_id=c.DEPTH_FRAME,
    width=c.WIDTH,
    height=c.HEIGHT,
    compressed_bytes_file="compressed/depth16.jxl",
    note="decodes to a single-channel uint16 image (16UC1 depth, mm)",
    expected={"lossless": True, **c.expected_for_raw(c.depth16_pattern(), "16UC1")},
)
assert np.array_equal(imagecodecs.jpegxl_decode(back.data.tobytes()), c.depth16_pattern())

# --- PointCloud2 ---
xyz, intensity = c.point_cloud()
for name, names, columns in [
    ("pointcloud_xyzi", ["x", "y", "z", "intensity"], [xyz, intensity[:, None]]),
    ("pointcloud_xyz", ["x", "y", "z"], [xyz]),
]:
    points = np.ascontiguousarray(np.hstack(columns).astype("<f4"))
    point_step = 4 * len(names)
    fields = [PointField(name=n, offset=4 * k, datatype=7, count=1) for k, n in enumerate(names)]
    msg = RosPointCloud2(
        header=header(c.LIDAR_FRAME),
        height=1,
        width=c.N_POINTS,
        fields=fields,
        is_bigendian=False,
        point_step=point_step,
        row_step=point_step * c.N_POINTS,
        data=np.frombuffer(points.tobytes(), dtype=np.uint8),
        is_dense=True,
    )
    back = write(
        name,
        msg,
        "sensor_msgs/msg/PointCloud2",
        format="pointcloud",
        zenoh_key=zenoh_key("lidar/points", "sensor_msgs/msg/PointCloud2"),
        ros_topic="/lidar/points",
        frame_id=c.LIDAR_FRAME,
        point_count=c.N_POINTS,
        height=1,
        width=c.N_POINTS,
        point_step=point_step,
        fields=[{"name": n, "offset": 4 * k, "datatype": 7, "count": 1} for k, n in enumerate(names)],
        expected={
            "first_points": c.first_points(xyz, intensity if len(names) == 4 else None),
            "formula": "x=(i%200)*0.05, y=(i//200)*0.05, z=(i%7)*0.125" + (", intensity=i%256" if len(names) == 4 else ""),
            "min_xyz": [float(v) for v in xyz.min(axis=0)],
            "max_xyz": [float(v) for v in xyz.max(axis=0)],
        },
    )
    assert [f.name for f in back.fields] == names and back.point_step == point_step
    got = np.frombuffer(back.data.tobytes(), dtype="<f4").reshape(c.N_POINTS, len(names))
    assert np.array_equal(got, points), name

(HERE / "manifest.ros2.json").write_text(json.dumps({"entries": entries, "rihs01": type_hashes}, indent=2))
print(f"ros2: wrote and round-tripped {len(entries)} payloads")
