from common.retry import retry

attempt_count = 0


@retry(attempts=3, base_delay=1)
def unstable_operation():
    global attempt_count
    attempt_count += 1

    print(f"Attempt: {attempt_count}")

    if attempt_count < 3:
        raise ConnectionError("Temporary service failure")

    return "Operation successful!"


result = unstable_operation()

print(result)