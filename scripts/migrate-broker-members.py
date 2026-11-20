#!/usr/bin/env python3
"""Submit a durable Broker replacement and wait before retiring old Pods."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pd-url", required=True, help="PD HTTP origin, e.g. http://127.0.0.1:8080")
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--add", required=True, help="Comma-separated new Broker IDs")
    parser.add_argument("--remove", required=True, help="Comma-separated old Broker IDs")
    parser.add_argument("--change-id", help="Resume waiting for an already submitted change")
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    added = set(filter(None, (s.strip() for s in args.add.split(","))))
    removed = set(filter(None, (s.strip() for s in args.remove.split(","))))
    if not added or len(added) != len(removed) or added & removed or args.timeout <= 0:
        parser.error("add/remove must be nonempty, disjoint and equal in size; timeout must be positive")
    cluster = urllib.parse.quote(args.cluster, safe="")
    base = args.pd_url.rstrip("/") + "/api/v1/broker/clusters/" + cluster

    def request(path, body=None):
        req = urllib.request.Request(
            base + path,
            method="GET" if body is None else "PATCH",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.load(response)

    config = request("/configuration")
    roster = set(config["voterRoster"])
    expected = (roster - removed) | added
    change_id = args.change_id
    if not change_id:
        topology = request("/topology")
        brokers = {b["brokerId"]: b for b in topology["brokers"]}
        if not removed <= roster or added & roster:
            raise RuntimeError("old IDs must be current roster members and new IDs must be outside it")
        if any(brokers.get(b, {}).get("status") != "ONLINE" for b in added | removed):
            raise RuntimeError("all old and new Brokers must remain ONLINE before replacement")
        if not topology["partitions"] or any(not p.get("serviceReady") for p in topology["partitions"]):
            raise RuntimeError("every partition must be service-ready before replacement")
        result = request("/configuration", {
            "brokers": {"add": sorted(added), "remove": sorted(removed), "count": len(roster)}
        })
        change_id = result["changeId"]
        print(json.dumps({"changeId": change_id, "message": "Keep old Pods alive until COMPLETED."}), flush=True)
    deadline = time.monotonic() + args.timeout
    last = None
    while time.monotonic() < deadline:
        try:
            change = request("/changes/" + urllib.parse.quote(change_id, safe=""))
            topology = request("/topology")
            config = request("/configuration")
        except (OSError, urllib.error.HTTPError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code not in (502, 503, 504):
                raise
            time.sleep(1)
            continue
        status = change["status"]
        progress = {"changeId": change_id, "status": status, "pendingChanges": change.get("pendingChanges", [])}
        if progress != last:
            print(json.dumps(progress), flush=True)
            last = progress
        if status in ("FAILED", "CANCELLED", "SUPERSEDED"):
            raise RuntimeError(f"change {change_id} ended as {status}; keep old Pods alive and inspect history")
        partitions = topology["partitions"]
        brokers = {b["brokerId"]: b for b in topology["brokers"]}
        ready = (
            len(partitions) == config["partitionSize"] and set(config["voterRoster"]) == expected
            and all(p.get("serviceReady") and p["status"] == "ACTIVE"
                    and not removed & (set(p["replicas"]) | set(p["learners"]) | {p["leaderId"]})
                    and len(set(p["replicas"]) | {p["leaderId"]}) == config["replicationFactor"]
                    and all(brokers.get(b, {}).get("status") == "ONLINE"
                            for b in set(p["replicas"]) | {p["leaderId"]})
                    and (set(p["replicas"]) | {p["leaderId"]}) <= expected
                    for p in partitions)
        )
        if status == "COMPLETED" and not change.get("pendingChanges") and ready:
            print(json.dumps({"changeId": change_id, "status": "COMPLETED",
                              "retireBrokerIds": sorted(removed)}), flush=True)
            return
        time.sleep(1)
    raise RuntimeError(f"change {change_id} has not completed; keep old Pods alive, resume with --change-id {change_id}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
