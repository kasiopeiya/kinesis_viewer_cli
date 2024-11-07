import random


def get_random_string(n: int = 10) -> str:
    """ランダム文字列を生成"""
    start = ord("a")
    end = ord("z")

    tmp = [chr(random.randint(start, end)) for _ in range(n)]
    return "".join(tmp)
