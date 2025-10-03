def calculate_pi(num_terms):
    """Calculate an approximation of Pi
    """
    import time
    start_time = time.time()
    pi_over_4 = 0.0
    for k in range(num_terms):
        pi_over_4 += ((-1) ** k) / (2 * k + 1)
    end_time = time.time()
    pi_value = pi_over_4 * 4
    print(f"Calculated Pi in {end_time - start_time:.3f} seconds")
    return pi_value