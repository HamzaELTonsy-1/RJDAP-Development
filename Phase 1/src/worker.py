import argparse
import json
import socket

from protocol import (
    WORKER_JOIN,
    TASK_DISPATCH,
    TASK_RESULT,
    ACK_RESULT,
    JOIN_ACK,
    encode_message,
    decode_message,
)


def calculate_task(payload):
    if payload.get("operation") != "sum_squares":
        raise ValueError("Unsupported operation")

    numbers = payload.get("numbers", [])
    return sum(number * number for number in numbers)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    worker_id = args.id
    server_address = (args.host, args.port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5.0)

    join_message = encode_message(
        WORKER_JOIN,
        worker_id=worker_id,
        payload={"status": "joining"},
    )

    sock.sendto(join_message, server_address)
    print(f"[WORKER {worker_id}] Registration sent", flush=True)

    # Wait for registration acknowledgement.
    while True:
        try:
            data, sender = sock.recvfrom(2048)
        except socket.timeout:
            sock.sendto(join_message, server_address)
            print(f"[WORKER {worker_id}] Retrying registration", flush=True)
            continue

        if sender != server_address:
            continue

        try:
            message = decode_message(data)
        except (ValueError, json.JSONDecodeError):
            continue

        if message["message_type"] == JOIN_ACK and message["worker_id"] == worker_id:
            break

    print(f"[WORKER {worker_id}] Registered", flush=True)

    while True:
        try:
            data, sender = sock.recvfrom(2048)
        except socket.timeout:
            continue

        if sender != server_address:
            continue

        try:
            message = decode_message(data)
        except (ValueError, json.JSONDecodeError):
            continue

        if message["message_type"] != TASK_DISPATCH:
            continue

        if message["worker_id"] != worker_id:
            continue

        task_id = message["task_id"]
        attempt_num = message["attempt_num"]
        generation = message["generation"]
        receipt_tag = message["receipt_tag"]

        try:
            result = calculate_task(message["payload"])
        except ValueError as error:
            print(f"[WORKER {worker_id}] {error}", flush=True)
            continue

        print(
            f"[WORKER {worker_id}] Task {task_id}: "
            f"result={result}, generation={generation}",
            flush=True,
        )

        result_message = encode_message(
            TASK_RESULT,
            task_id=task_id,
            worker_id=worker_id,
            attempt_num=attempt_num,
            generation=generation,
            receipt_tag=receipt_tag,
            payload={"result": result},
        )

        # Send the result and retry until its matching ACK arrives.
        while True:
            sock.sendto(result_message, server_address)

            try:
                ack_data, ack_sender = sock.recvfrom(2048)
            except socket.timeout:
                print(
                    f"[WORKER {worker_id}] Retrying result for task {task_id}",
                    flush=True,
                )
                continue

            if ack_sender != server_address:
                continue

            try:
                ack = decode_message(ack_data)
            except (ValueError, json.JSONDecodeError):
                continue

            if (
                ack["message_type"] == ACK_RESULT
                and ack["task_id"] == task_id
                and ack["worker_id"] == worker_id
                and ack["generation"] == generation
                and ack["receipt_tag"] == receipt_tag
            ):
                print(
                    f"[WORKER {worker_id}] ACK received for task {task_id}",
                    flush=True,
                )
                break


if __name__ == "__main__":
    main()
