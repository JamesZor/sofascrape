import logging
import random
import time
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

# ==========================================
# MATHEMATICAL DELAY STRATEGIES
# ==========================================


def _gamma_delay(alpha: float, beta: float) -> float:
    """
    Long-tail distribution. Usually fast, occasionally very slow.
    Mean sleep time = alpha * beta.
    Example: alpha=2.5, beta=0.8 -> Mean ~2s, but can naturally spike to 7s+.
    """
    return random.gammavariate(alpha, beta)


def _normal_delay(mu: float, sigma: float) -> float:
    """
    Standard bell curve.
    Example: mu=2.0, sigma=0.5 -> 95% of sleeps will be between 1.0s and 3.0s.
    """
    # Use max(0.1, ...) to ensure we never calculate a negative sleep time
    return max(0.1, random.gauss(mu, sigma))


def _uniform_delay(min_val: float, max_val: float) -> float:
    """
    Equal probability of any time between min and max.
    Example: min=1.0, max=3.0.
    """
    return random.uniform(min_val, max_val)


def _constant_delay(value: float) -> float:
    """
    The classic, predictable wait. (Not recommended for stealth).
    """
    return value


# ==========================================
# THE REGISTRY
# ==========================================

SLEEPER_REGISTRY: Dict[str, Callable[..., float]] = {
    "gamma": _gamma_delay,
    "normal": _normal_delay,
    "uniform": _uniform_delay,
    "constant": _constant_delay,
}

# ==========================================
# MAIN EXECUTION WRAPPER
# ==========================================


def smart_sleep(strategy: str, params: Dict[str, Any]) -> float:
    """
    Calculates the delay based on the chosen strategy, sleeps, and returns the duration.
    """
    func = SLEEPER_REGISTRY.get(strategy.lower())

    if not func:
        logger.warning(
            f"Sleep strategy '{strategy}' not found. Falling back to constant 2.0s."
        )
        func = _constant_delay
        params = {"value": 2.0}

    # Calculate the time using the unpacked parameters
    duration = func(**params)

    logger.debug(f"Sleeping for {duration:.2f}s (Strategy: {strategy})")
    time.sleep(duration)

    return duration
