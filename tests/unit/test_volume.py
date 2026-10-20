from drct.monitoring.volume import AnomalyResult, detect_anomaly

HISTORY = [300, 310, 295, 305, 290, 315, 300, 298]


def test_spike_is_anomalous():
    r = detect_anomaly(HISTORY, 2400)
    assert r.is_anomaly and r.reason == "spike"
    assert r.robust_z is not None and r.robust_z > 3.5


def test_drop_is_anomalous():
    r = detect_anomaly(HISTORY, 10)
    assert r.is_anomaly and r.reason == "drop"
    assert r.robust_z is not None and r.robust_z < -3.5


def test_normal_day_is_not():
    r = detect_anomaly(HISTORY, 320)
    assert not r.is_anomaly and r.reason == "normal"
    assert r.baseline_median == 300 and r.mad == 5


def test_zero_mad_and_short_history_do_not_crash_or_flag():
    assert not detect_anomaly([300] * 10, 305).is_anomaly
    r = detect_anomaly([300, 300], 5000)
    assert not r.is_anomaly and r.reason == "insufficient_history"


def test_zero_mad_falls_back_to_five_percent_of_median():
    r = detect_anomaly([300] * 10, 305)
    assert r.mad == 15.0  # max(1.0, 0.05 * 300)
    assert r.robust_z == 0.6745 * 5 / 15.0


def test_zero_mad_still_flags_a_large_departure():
    assert detect_anomaly([300] * 10, 5000).is_anomaly


def test_insufficient_history_has_no_score():
    r = detect_anomaly([300] * 6, 5000)
    assert r == AnomalyResult(
        value=5000,
        baseline_median=None,
        mad=None,
        robust_z=None,
        is_anomaly=False,
        reason="insufficient_history",
    )


def test_min_history_boundary_is_inclusive():
    assert detect_anomaly([300] * 7, 5000).is_anomaly


def test_threshold_is_exclusive():
    # median 300, MAD 5 -> z = 0.6745 * 26 / 5 = 3.507 > 3.5; z = 0.6745 * 25 / 5 = 3.37 <= 3.5
    assert detect_anomaly(HISTORY, 326).is_anomaly
    assert not detect_anomaly(HISTORY, 325).is_anomaly


# Live clean-data history (eight Fridays of raw.sales) whose 8-sample MAD came out at 15.5,
# only 2% of the median: an ordinary +12% Friday (864, earlier Fridays reached 897) scored z=4.05.
TIGHT_FRIDAYS = [769, 762, 795, 738, 870, 784, 773, 753]


def test_tight_mad_flags_an_ordinary_wobble_by_default():
    r = detect_anomaly(TIGHT_FRIDAYS, 864)
    assert r.mad == 15.5 and r.is_anomaly


def test_min_mad_ratio_floors_a_too_tight_mad():
    r = detect_anomaly(TIGHT_FRIDAYS, 864, min_mad_ratio=0.05)
    assert r.mad == 0.05 * 771
    assert not r.is_anomaly and r.reason == "normal"


def test_min_mad_ratio_still_flags_spikes_and_drops():
    assert detect_anomaly(TIGHT_FRIDAYS, 864 * 8, min_mad_ratio=0.05).reason == "spike"
    assert detect_anomaly(TIGHT_FRIDAYS, 80, min_mad_ratio=0.05).reason == "drop"


def test_min_mad_ratio_never_narrows_a_wide_mad():
    wide = [100, 150, 200, 250, 300, 350, 400]
    assert detect_anomaly(wide, 260, min_mad_ratio=0.05).mad == detect_anomaly(wide, 260).mad
