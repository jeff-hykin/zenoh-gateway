# A synthetic dimos camera for bench/latency.ts --pixel-stamp: 1280x720 rgb8 sensor_msgs.Image (LCM) at HZ on KEY, its
# header stamp = now, and that stamp (unix ms, low 40 bits) burned into the top 32 rows as 40 black/white 32-px blocks
# (bit 39 first) over a moving texture (so the encoder works as on a real scene).
#   python bench/camera.py bench/color_image/sensor_msgs.Image 15 [zenoh config insertions as JSON]
# Default zenoh config: dimos's own (peer, listening on 127.0.0.1, scouting on loopback; macOS: lo0, else lo).
import json, struct, sys, time, platform
import numpy as np
import zenoh

key, hz = sys.argv[1], float(sys.argv[2])
loopback = "lo0" if platform.system() == "Darwin" else "lo"
insertions = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {"mode": "peer", "listen/endpoints": ["tcp/127.0.0.1:0"], "scouting/multicast/interface": loopback}
WIDTH, HEIGHT = 1280, 720
texture = np.random.default_rng(1).integers(0, 255, (HEIGHT // 8, WIDTH // 8 * 2, 3), dtype=np.uint8).repeat(8, 0).repeat(8, 1)
# dimos-lcm sensor_msgs.Image's fingerprint
FINGERPRINT = bytes.fromhex("535cface1f4f5717")
FRAME_ID = b"camera_optical\0"

config = zenoh.Config()
for name, value in insertions.items():
    config.insert_json5(name, json.dumps(value))
session = zenoh.open(config)
publisher = session.declare_publisher(key)
count = 0
while True:
    now = time.time()
    shift = (count * 8) % WIDTH
    picture = np.ascontiguousarray(texture[:, shift:shift + WIDTH])
    stamp_ms = int(now * 1000) & ((1 << 40) - 1)
    for bit in range(40):
        picture[0:32, bit * 32:(bit + 1) * 32] = 255 if (stamp_ms >> (39 - bit)) & 1 else 0
    data = picture.tobytes()
    header = FINGERPRINT + struct.pack(">iiii", len(data), count, int(now), int((now % 1) * 1e9)) + struct.pack(">i", len(FRAME_ID)) + FRAME_ID
    header += struct.pack(">ii", HEIGHT, WIDTH) + struct.pack(">i", 5) + b"rgb8\0" + b"\0" + struct.pack(">i", WIDTH * 3)
    publisher.put(header + data)
    count += 1
    time.sleep(max(0.0, 1 / hz - (time.time() - now)))
