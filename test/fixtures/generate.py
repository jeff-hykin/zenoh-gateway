#!/usr/bin/env python3
"""Regenerate every fixture: dimos LCM payloads (dimos venv), then ROS 2 CDR payloads (rosbags env), then manifest.json.

Usage: python3 generate.py [--dimos-python PATH] [--ros-python PATH]
  --dimos-python  default ~/repos/dimos/.venv/bin/python
  --ros-python    default: an ephemeral `uv run --with rosbags,...` env (any python with rosbags numpy pillow imagecodecs works)
"""

import argparse
import json
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
ROS_DEPS = ["rosbags", "numpy", "pillow", "imagecodecs"]

parser = argparse.ArgumentParser()
parser.add_argument("--dimos-python", default=str(Path.home() / "repos/dimos/.venv/bin/python"))
parser.add_argument("--ros-python", default=None)
args = parser.parse_args()

for sub in ("dimos", "ros2", "compressed"):
    shutil.rmtree(HERE / sub, ignore_errors=True)
    (HERE / sub).mkdir()

subprocess.run([args.dimos_python, str(HERE / "gen_dimos.py")], check=True)
ros_cmd = [args.ros_python] if args.ros_python else ["uv", "run", "--no-project", "--python", "3.12", *sum((["--with", d] for d in ROS_DEPS), []), "python"]
subprocess.run([*ros_cmd, str(HERE / "gen_ros2.py")], check=True)

dimos_entries = json.loads((HERE / "manifest.dimos.json").read_text())
ros = json.loads((HERE / "manifest.ros2.json").read_text())
manifest = {
    "description": "zenoh-web codec fixtures: one payload file per zenoh sample (the sample's raw payload bytes)",
    "pattern": {
        "image": "320x240, four 160x120 solid quadrants: top_left red, top_right green, bottom_left blue, bottom_right white",
        "mono8": "quadrants 0 / 85 / 170 / 255 (TL, TR, BL, BR)",
        "mono16": "quadrants 0 / 21845 / 43690 / 65535 (TL, TR, BL, BR)",
        "depth_16UC1": "1000 + col + 4*row (mm), little-endian",
        "depth_32FC1": "0.5 + col/128 + row/64 (m), little-endian, exact in float32",
        "pointcloud": "20000 points, x=(i%200)*0.05, y=(i//200)*0.05, z=(i%7)*0.125, intensity=i%256, float32 LE",
        "stamp": {"sec": 1700000000, "nanosec": 500000000},
    },
    "key_formats": {
        "dimos-lcm": "<topic>/<msg_name>  (dimos ZenohPubSub Topic.key_expr), payload = LCM encoding incl. 8-byte fingerprint",
        "ros2-cdr": "<domain_id>/<topic>/<pkg>::msg::dds_::<Type>_/RIHS01_<sha256> (rmw_zenoh); payload = CDR with 4-byte encapsulation header. rmw_zenoh also sends a zenoh attachment (seq, timestamp, gid) not stored here",
    },
    "rihs01_jazzy": ros["rihs01"],
    "rihs01_source": "computed by rosbags Typestore(ROS2_JAZZY).hash_rihs01 (std_msgs/String matches the known Jazzy hash df668c74...)",
    "entries": dimos_entries + ros["entries"],
}
(HERE / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
(HERE / "manifest.dimos.json").unlink()
(HERE / "manifest.ros2.json").unlink()
print(f"manifest.json: {len(manifest['entries'])} entries")
