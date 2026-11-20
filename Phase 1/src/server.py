import argparse
import json
import secrets
import socket

from protocol import (
    WORKER_JOIN,
    HEARTBEAT,
    TASK_DISPATCH,
    TASK_RESULT,
    ACK_RESULT,
    JOIN_ACK,
    encode_message,
    decode_message,
)


def calculate_expected(numbers):
    return sum(number * number for number in numbers)


def validate_assignment(message, assignment):
    if message["worker_id"] != assignment["worker_id"]:
        return False, "wrong worker"

    if message["generation"] != assignment["generation"]:
        return False, "stale generation"

    if message["receipt_tag"] != assignment["receipt_tag"]:
        return False, "invalid receipt tag"

    return True, "valid"


def process_result(message, assignment, completed_tasks):
    valid, reason = validate_assignment(message, assignment)

    if not valid:
        return "rejected", reason

    task_id = message["task_id"]
    result = message["payload"].get("result")

    if result != assignment["expected_result"]:
        return "rejected", "incorrect result"

    if task_id in completed_tasks:
        return "duplicate", "already completed"

    completed_tasks.add(task_id)
    return "accepted", "result recorded"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--tasks", type=int, default=1000)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    sock.settimeout(1.0)

    print(f"[SERVER] Listening on {args.host}:{args.port}", flush=True)

    workers = {}
    completed_tasks = set()
    sequence_num = 0
    duplicate_results = 0

    # Register workers.
    while len(workers) < args.workers:
        try:
            data, address = sock.recvfrom(2048)
        except socket.timeout:
            continue

        try:
            message = decode_message(data)
        except (ValueError, json.JSONDecodeError):
            continue

        if message["message_type"] != WORKER_JOIN:
            continue

        worker_id = message["worker_id"]

        if worker_id < 1 or worker_id > args.workers:
            continue

        workers[worker_id] = address

        response = encode_message(
            JOIN_ACK,
            worker_id=worker_id,
            sequence_num=sequence_num,
            payload={"status": "registered"},
        )
        sequence_num += 1
        sock.sendto(response, address)

        print(f"[SERVER] Worker {worker_id} registered", flush=True)

    print("[SERVER] All workers registered", flush=True)

    # Store the active assignment for each task.
    assignments = {}

    for task_id in range(1, args.tasks + 1):
        worker_id = ((task_id - 1) % args.workers) + 1
        address = workers[worker_id]

        numbers = [task_id, task_id + 1, task_id + 2]
        expected_result = calculate_expected(numbers)

        generation = 1
        attempt_num = 1
        receipt_tag = secrets.randbits(32)

        assignments[task_id] = {
            "worker_id": worker_id,
            "generation": generation,
            "receipt_tag": receipt_tag,
            "expected_result": expected_result,
        }

        dispatch = encode_message(
            TASK_DISPATCH,
            task_id=task_id,
            worker_id=worker_id,
            attempt_num=attempt_num,
            generation=generation,
            receipt_tag=receipt_tag,
            sequence_num=sequence_num,
            payload={
                "operation": "sum_squares",
                "numbers": numbers,
            },
        )
        sequence_num += 1
        sock.sendto(dispatch, address)

        print(
            f"[SERVER] Dispatched task {task_id} to worker {worker_id} "
            f"(generation={generation})",
            flush=True,
        )

        while True:
            try:
                data, sender = sock.recvfrom(2048)
            except socket.timeout:
                continue

            try:
                message = decode_message(data)
            except (ValueError, json.JSONDecodeError):
                continue

            if message["message_type"] == HEARTBEAT:
                continue

            if message["message_type"] != TASK_RESULT:
                continue

            received_task_id = message["task_id"]

            # Handle retransmissions of already completed tasks first.
            if received_task_id in completed_tasks:
                assignment = assignments[received_task_id]

                status, reason = process_result(message, assignment, completed_tasks)

                # Only acknowledge a valid retransmission
                # from the original assigned worker.
                if status != "duplicate" or sender != workers[assignment["worker_id"]]:
                    print(
                        f"[SERVER] Rejected duplicate: {reason}",
                        flush=True,
                    )
                    continue

                duplicate_results += 1

                ack = encode_message(
                    ACK_RESULT,
                    task_id=received_task_id,
                    worker_id=assignment["worker_id"],
                    attempt_num=1,
                    generation=assignment["generation"],
                    receipt_tag=assignment["receipt_tag"],
                    sequence_num=sequence_num,
                    payload={"status": "already_accepted"},
                )
                sequence_num += 1

                sock.sendto(ack, sender)

                print(
                    f"[SERVER] Duplicate acknowledged for task {received_task_id}",
                    flush=True,
                )
                continue

            # Validate a new result against the current assignment.
            if received_task_id != task_id or sender != address:
                continue

            assignment = assignments[task_id]

            status, reason = process_result(message, assignment, completed_tasks)

            if status != "accepted":
                print(
                    f"[SERVER] Rejected task {task_id}: {reason}",
                    flush=True,
                )
                continue

            acknowledgement = encode_message(
                ACK_RESULT,
                task_id=task_id,
                worker_id=worker_id,
                attempt_num=attempt_num,
                generation=generation,
                receipt_tag=receipt_tag,
                sequence_num=sequence_num,
                payload={"status": "accepted"},
            )
            sequence_num += 1

            sock.sendto(acknowledgement, sender)

            print(f"[SERVER] Task {task_id} completed", flush=True)
            break

    print("\n========== BASELINE SUMMARY ==========", flush=True)
    print(f"Tasks requested: {args.tasks}", flush=True)
    print(f"Tasks completed: {len(completed_tasks)}", flush=True)
    print(f"Duplicate results detected: {duplicate_results}", flush=True)
    print("======================================", flush=True)

    sock.close()


if __name__ == "__main__":
    main()
