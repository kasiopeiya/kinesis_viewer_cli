import functools
import random


def get_random_string(n: int = 10) -> str:
    """ランダム文字列を生成"""
    start = ord("a")
    end = ord("z")

    tmp = [chr(random.randint(start, end)) for _ in range(n)]
    return "".join(tmp)


def error_handling(func):
    """例外をキャッチし、関数名と例外内容を出力するデコレータ"""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            function_name = func.__name__
            print(f"Error during {function_name}: {e}")
            raise e

    return wrapper
