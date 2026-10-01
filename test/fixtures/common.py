"""Shared test patterns and expected-content helpers (numpy only, used by both generator stages)."""

import numpy as np

WIDTH, HEIGHT = 320, 240
TS = 1700000000.5  # fixed stamp: sec=1700000000, nanosec=500000000
COLOR_FRAME = "camera_color_optical_frame"
DEPTH_FRAME = "camera_depth_optical_frame"
LIDAR_FRAME = "lidar"

# quadrant -> RGB; quadrants are 160x120 each
QUADRANT_RGB = {
    "top_left": (255, 0, 0),
    "top_right": (0, 255, 0),
    "bottom_left": (0, 0, 255),
    "bottom_right": (255, 255, 255),
}
QUADRANT_MONO8 = {"top_left": 0, "top_right": 85, "bottom_left": 170, "bottom_right": 255}
QUADRANT_MONO16 = {"top_left": 0, "top_right": 21845, "bottom_left": 43690, "bottom_right": 65535}

N_POINTS = 20000
FIRST_N = 8


def quadrant_slices(height=HEIGHT, width=WIDTH):
    h2, w2 = height // 2, width // 2
    return {
        "top_left": (slice(0, h2), slice(0, w2)),
        "top_right": (slice(0, h2), slice(w2, width)),
        "bottom_left": (slice(h2, height), slice(0, w2)),
        "bottom_right": (slice(h2, height), slice(w2, width)),
    }


def _fill(values, dtype, channels):
    shape = (HEIGHT, WIDTH, channels) if channels > 1 else (HEIGHT, WIDTH)
    arr = np.zeros(shape, dtype=dtype)
    for name, (rows, cols) in quadrant_slices().items():
        arr[rows, cols] = values[name]
    return arr


def rgb_pattern():
    return _fill(QUADRANT_RGB, np.uint8, 3)


def bgr_pattern():
    return rgb_pattern()[:, :, ::-1].copy()


def mono8_pattern():
    return _fill(QUADRANT_MONO8, np.uint8, 1)


def mono16_pattern():
    return _fill(QUADRANT_MONO16, np.uint16, 1)


def depth16_pattern():
    """16UC1 millimetres: 1000 + col + 4*row (range 1000..2275)."""
    rows, cols = np.mgrid[0:HEIGHT, 0:WIDTH]
    return (1000 + cols + 4 * rows).astype(np.uint16)


def depth32_pattern():
    """32FC1 metres: 0.5 + col/128 + row/64, every value exactly representable in float32."""
    rows, cols = np.mgrid[0:HEIGHT, 0:WIDTH]
    return (0.5 + cols / 128.0 + rows / 64.0).astype(np.float32)


DEPTH_SAMPLE_PIXELS = [(0, 0), (0, 319), (239, 0), (239, 319), (120, 160), (17, 42)]  # (row, col)


def depth_samples(arr):
    return [{"row": r, "col": c, "value": arr[r, c].item()} for r, c in DEPTH_SAMPLE_PIXELS]


def quadrant_means(arr):
    """Per-quadrant mean of each channel (list for multi-channel, scalar for mono)."""
    out = {}
    for name, (rows, cols) in quadrant_slices(arr.shape[0], arr.shape[1]).items():
        region = arr[rows, cols].astype(np.float64)
        if region.ndim == 3:
            out[name] = [round(float(v), 3) for v in region.reshape(-1, region.shape[2]).mean(axis=0)]
        else:
            out[name] = round(float(region.mean()), 3)
    return out


def overall_mean_rgb(arr):
    return [round(float(v), 3) for v in arr.reshape(-1, arr.shape[-1]).astype(np.float64).mean(axis=0)]


def point_cloud():
    """20k-point grid: x=(i%200)*0.05, y=(i//200)*0.05, z=(i%7)*0.125, intensity=i%256 (all exact float32)."""
    i = np.arange(N_POINTS)
    xyz = np.stack([(i % 200) * 0.05, (i // 200) * 0.05, (i % 7) * 0.125], axis=1).astype(np.float32)
    intensity = (i % 256).astype(np.float32)
    return xyz, intensity


def first_points(xyz, intensity=None, n=FIRST_N):
    rows = []
    for k in range(n):
        row = {"x": float(xyz[k, 0]), "y": float(xyz[k, 1]), "z": float(xyz[k, 2])}
        if intensity is not None:
            row["intensity"] = float(intensity[k])
        rows.append(row)
    return rows


def raw_image_cases():
    """(case name, array, ros encoding, frame_id, topic suffix)."""
    return [
        ("image_rgb8", rgb_pattern(), "rgb8", COLOR_FRAME, "camera/color"),
        ("image_bgr8", bgr_pattern(), "bgr8", COLOR_FRAME, "camera/color"),
        ("image_mono8", mono8_pattern(), "mono8", COLOR_FRAME, "camera/mono"),
        ("image_mono16", mono16_pattern(), "mono16", COLOR_FRAME, "camera/mono"),
        ("depth_16UC1", depth16_pattern(), "16UC1", DEPTH_FRAME, "camera/depth"),
        ("depth_32FC1", depth32_pattern(), "32FC1", DEPTH_FRAME, "camera/depth"),
    ]


def expected_for_raw(arr, encoding):
    exp = {}
    if encoding in ("rgb8", "bgr8"):
        rgb = arr if encoding == "rgb8" else arr[:, :, ::-1]
        exp["mean_rgb"] = overall_mean_rgb(rgb)
        exp["quadrant_mean_rgb"] = quadrant_means(rgb)
        exp["pixel_bytes_at_row0_col0"] = [int(v) for v in arr[0, 0]]
    elif encoding in ("mono8", "mono16"):
        exp["quadrant_mean"] = quadrant_means(arr)
    else:
        exp["depth_samples"] = depth_samples(arr)
        exp["min"] = arr.min().item()
        exp["max"] = arr.max().item()
        exp["formula"] = (
            "value = 1000 + col + 4*row (mm)" if encoding == "16UC1" else "value = 0.5 + col/128 + row/64 (m)"
        )
    return exp
