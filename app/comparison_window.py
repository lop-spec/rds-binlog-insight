"""Analysis comparison windows; units are inherited (seconds or microseconds)."""
def previous_window(start, end):
    if end <= start:
        raise ValueError('invalid_comparison_window')
    return start - (end - start), start


def comparison_window(start, end, explicit_start=None):
    base, stop = previous_window(start, end)
    if explicit_start is not None:
        base = explicit_start
        stop = base + end - start
    if stop > start:
        raise ValueError('baseline_must_not_overlap_current_window')
    return base, stop
