def avoid_zero_value(val, zero=0.01):
    return zero if val == 0 else val


def max_value(val, max=0.99):
    return max if val >= 1 else val


def expand_odds(odds):
    coeff = 1 / sum(odds)
    return [x * coeff for x in odds]


def min_value(val, min=0.01):
    return min if val <= 0 else val


def calibrate_chanses(parts, original_accuracies):
    accuracies = [0.5 for x in original_accuracies]
    total_parts = sum(parts)
    total_accuracies = sum(accuracies)
    if total_accuracies == 0:
        return parts
    exceeded = 1 - total_parts
    relative_distribution = [(part / total_parts) for part in parts]
    relative_fails = [((1 - accuracy) / total_accuracies)
                      for accuracy in accuracies]
    fail_distribution = [(fail / sum(relative_fails))
                         for fail in relative_fails]
    parts_to_add = [(fail_chance + relative_distribution[i]) / 2 * exceeded for i,
                    fail_chance in enumerate(fail_distribution)]
    distributed_chanses = [avoid_zero_value(
        (part + parts_to_add[i]) // 0.01 / 100) for i, part in enumerate(parts)]
    for i, chance in enumerate(distributed_chanses):
        if chance < 0:
            distributed_chanses[i] = 0.01
            accuracies[i] = 1
            distributed_chanses = calibrate_chanses(
                distributed_chanses, accuracies)
            break
    return distributed_chanses
