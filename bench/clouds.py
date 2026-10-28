# Synthetic point clouds for bench/latency.ts: copies of a dimos sensor_msgs.PointCloud2 (LCM) message at HZ on each KEY,
# its header stamp set to now (e.g. zenoh-dimos-codecs' test/fixtures/dimos/pointcloud_xyzi.bin, 320 KB, 20k points).
#   python bench/clouds.py pointcloud_xyzi.bin 10 bench/lidar/sensor_msgs.PointCloud2 bench/cloud/sensor_msgs.PointCloud2
# zenoh config: dimos's own (peer, listening on 127.0.0.1, scouting on loopback; macOS: lo0, else lo).
import struct, sys, time, platform
import zenoh

path, hz, keys = sys.argv[1], float(sys.argv[2]), sys.argv[3:]
message = bytearray(open(path, "rb").read())
# fingerprint (8) | field count (4) | data length (4) | header: seq (4), stamp sec (4), nsec (4)
STAMP_OFFSET = 20
config = zenoh.Config()
config.insert_json5("mode", '"peer"')
config.insert_json5("listen/endpoints", '["tcp/127.0.0.1:0"]')
config.insert_json5("scouting/multicast/interface", '"lo0"' if platform.system() == "Darwin" else '"lo"')
session = zenoh.open(config)
publishers = [session.declare_publisher(key) for key in keys]
while True:
    now = time.time()
    struct.pack_into(">ii", message, STAMP_OFFSET, int(now), int((now % 1) * 1e9))
    for publisher in publishers:
        publisher.put(bytes(message))
    time.sleep(max(0.0, 1 / hz - (time.time() - now)))
