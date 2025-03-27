from pathlib import Path

from cstar.base.common import ConfiguredBaseModel

DEFAULT_ROOT_PATH = Path("~/cstar/").expanduser()
DEFAULT_ACCOUNT = "cworthy"

def cast_str_args_to_path(func):
    def wrapper(*args, **kwargs):
        new_args = [Path(a) if isinstance(a, str) else a for a in args]
        new_kwargs = {k: (Path(v) if isinstance(v, str) else v) for k, v in kwargs.items()}
        return func(*new_args, **new_kwargs)

    return wrapper


class RunPath:


    def __init__(self, run_plan: str | None = None,  account: str = DEFAULT_ACCOUNT, root_path: str | Path = DEFAULT_ROOT_PATH):
        self.root_path = Path(root_path)


if __name__ == "__main__":
    # todo move to unit tests

    @cast_str_args_to_path
    def try_decorator(arg1, arg2, my_arg = None):
        print(repr(arg1))
        print(repr(arg2))
        print(repr(my_arg))


    try_decorator("hello", "~/foo", my_arg="~/bar/baz")