"""CSV utilities for file processing"""

import os


def split_csv_generator(file_path, chunk_size=4 * 1024 * 1024):
    """Generate CSV chunks preserving complete lines"""
    chunk_number = 1
    leftover = ""

    if not os.path.exists(file_path):
        yield None
        return

    with open(file_path, "r", encoding="utf-8") as file:
        while True:
            chunk_data = file.read(chunk_size)
            if not chunk_data:
                if leftover:
                    yield {
                        "chunk_number": chunk_number,
                        "chunk_text": leftover,
                        "size_bytes": len(leftover.encode("utf-8")),
                    }
                break

            chunk_text = leftover + chunk_data
            last_newline = chunk_text.rfind("\n")
            if last_newline != -1:
                complete_part = chunk_text[: last_newline + 1]
                leftover = chunk_text[last_newline + 1 :]
            else:
                complete_part = ""
                leftover = chunk_text

            if complete_part:
                yield {
                    "chunk_number": chunk_number,
                    "chunk_text": complete_part,
                    "size_bytes": len(complete_part.encode("utf-8")),
                }
                chunk_number += 1


def count_chunks(file_path, chunk_size=4 * 1024 * 1024):
    """Count total chunks in file"""
    if not os.path.exists(file_path):
        return 0
    return sum(1 for _ in split_csv_generator(file_path, chunk_size))


def count_csv_lines(file_path):
    """Count lines in CSV (excluding header)"""
    if not os.path.exists(file_path):
        return 0
    with open(file_path, "r", encoding="utf-8") as f:
        total = sum(1 for _ in f)
    return max(0, total - 1)
