"""The measurement loop - running every 30 minute mark"""

import datetime
import logging
import os

from . import clock, naming, prober, proc, rates, reporter, targets


def load_profiles(cfg):
    """Buckets every profile's targets by (protocol, port). A profile
    whose targets or probe rate don't validate is skipped with an error.
    """
    profiles = []
    for profile in cfg["scans"]:
        name = profile["name"]
        targets_path = targets.resolve_targets_path(profile, cfg["paths"])
        try:
            buckets = targets.bucket_targets(targets_path)
        except (targets.TargetFileError, OSError) as e:
            logging.error("[%s] Skipping profile: %s", name, e)
            continue
        if not buckets:
            logging.warning("[%s] Skipping profile: no targets in %s", name, targets_path)
            continue
        try:
            prober.resolve_probe_rates(profile, buckets, cfg)
        except rates.RateAllocationError as e:
            logging.error("[%s] Skipping profile: %s", name, e)
            continue
        profiles.append((profile, buckets))
    return profiles


def warn_if_short_on_cpus(profiles):
    widest = max(
        (len(buckets) for profile, buckets in profiles if profile.get("parallel")),
        default=1,
    )
    cpus = os.cpu_count() or 1
    if widest > 1 and widest + 1 > cpus:
        logging.warning(
            "Up to %d concurrent scans on %d cpu(s). yarrp busy-spins to hold its "
            "probe rate, so scans may under-deliver their allocated pps - check "
            "achieved_pps in the manifest.",
            widest, cpus,
        )


def run_cycle(cfg, cycle_start, deadline):
    profiles = load_profiles(cfg)
    if not profiles:
        logging.error("No runnable profiles this cycle")
        return
    warn_if_short_on_cpus(profiles)

    for profile, buckets in profiles:
        name = profile["name"]
        for bucket, ip_list in buckets.items():
            logging.info(
                "[%s] bucket %s: %d targets",
                name, naming.bucket_tag(*bucket), len(ip_list),
            )

    for profile, buckets in profiles:
        if proc.shutdown_requested():
            logging.warning("Shutting down - skipping remaining profiles")
            return
        entries = prober.run_profile(profile, buckets, cfg, cycle_start, deadline)
        succeeded = sum(1 for entry in entries if entry.get("succeeded"))
        logging.info(
            "[%s] Profile complete: %d/%d bucket(s) succeeded",
            profile["name"], succeeded, len(entries),
        )


def wait_for_first_boundary(interval):
    """
    VPs now wait until the next interval to launch a yarrp scan so that scans are
    guaranteed to happen on the boundaries. Makes bucketing and other server side logic
    much simpler at the cost of the occasional yarrp scan.
    """
    first = clock.ceil(clock.now(), interval)
    seconds = (first - clock.now()).total_seconds()
    if seconds <= 0:
        return False
    logging.info(
        "Waiting %.0fs for the first cycle boundary at %s",
        seconds, first.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )
    return proc.wait_for_shutdown(seconds)


def run_forever(cfg):
    interval = datetime.timedelta(seconds=cfg["prober"]["interval"])
    logging.info(
        "IODA Upstream Delay vantage point %s starting - interval %ds, %d profile(s)",
        cfg["vp"]["hostname"], cfg["prober"]["interval"], len(cfg["scans"]),
    )

    if wait_for_first_boundary(interval):
        logging.info("Shutdown complete")
        return

    while not proc.shutdown_requested():
        # Floored, not the wall clock: a few ms of wake-up lag would otherwise
        # put 163002 in the filename, and the server groups uploads into
        # windows by parsing exactly that field.
        cycle_start = clock.floor(clock.now(), interval)
        # Not clock.ceil(): cycle_start is already on a boundary, so ceil would
        # return it unchanged and leave the cycle a zero-second budget.
        deadline = cycle_start + interval
        try:
            reporter.report(cfg)
            logging.info(
                "=== Cycle starting %s (next cycle due %s) ===",
                cycle_start.strftime("%Y-%m-%d %H:%M:%S UTC"),
                deadline.strftime("%H:%M:%S"),
            )
            run_cycle(cfg, cycle_start, deadline)
            reporter.report(cfg)
        except Exception:
            logging.exception("Cycle failed")

        if proc.shutdown_requested():
            break

        now = clock.now()
        # Strictly in the future: ceil() of a time already on a boundary
        # returns it unchanged, which would be a zero sleep and a hot loop.
        next_run = clock.floor(now, interval) + interval
        sleep_seconds = (next_run - now).total_seconds()
        if next_run > deadline:
            logging.warning(
                "Cycle overran its interval - next cycle at %s",
                next_run.strftime("%H:%M:%S"),
            )
        logging.info("Sleeping %.0fs until %s", sleep_seconds, next_run.strftime("%H:%M:%S"))
        proc.wait_for_shutdown(sleep_seconds)

    logging.info("Shutdown complete")
