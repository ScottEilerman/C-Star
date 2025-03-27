from pathlib import Path

DEFAULT_ROOT_PATH = Path("~/cstar/").expanduser()
DEFAULT_ACCOUNT = "cworthy"


def cast_str_args_to_path(func):
    def wrapper(*args, **kwargs):
        new_args = [Path(a) if isinstance(a, str) else a for a in args]
        new_kwargs = {k: (Path(v) if isinstance(v, str) else v) for k, v in kwargs.items()}
        return func(*new_args, **new_kwargs)

    return wrapper

class UnspecifiedPathError(ValueError):
    def __init__(self, missing_param: str, *args, **kwargs):
        super().__init__()
        self.message = f"{missing_param} was not defined, so the requested path cannot be determined."

class RunPath:
    def __init__(
        self,
        run_plan: str | None = None,
        run_id: str | None = None,
        step: str | None = None,
        segment: int | None = None,
        account: str = DEFAULT_ACCOUNT,
        root_path: str | Path = DEFAULT_ROOT_PATH,
    ):
        self.root_path = Path(root_path)
        self.account = account
        self.run_plan = run_plan
        self.run_id = run_id
        self.step = step
        self.segment = segment

    @property
    def account_path(self):
        if self.account is None:
            raise UnspecifiedPathError("account")
        return self.root_path / self.account

    @property
    def run_plan_path(self):
        if self.run_plan is None:
            raise UnspecifiedPathError("run_plan")
        return self.account_path / self.run_plan

    @property
    def run_id_path(self):
        if self.run_plan is None:
            raise UnspecifiedPathError("run_id")
        return self.run_plan_path / self.run_id

    @property
    def step_path(self):
        if self.step is None:
            raise UnspecifiedPathError("step")
        return self.run_id_path / self.step

    @property
    def segment_path(self):
        # if segment is not defined, assume the step isn't split into segments,
        # and thus we skip that layer in the path
        if self.segment is None:
            return self.step_path
        return self.step_path / f"segment_{self.segment:04d}"

    @property
    def input_path(self):
        return self.segment_path / "input"

    @property
    def work_path(self):
        return self.segment_path / "work"

    @property
    def output_path(self):
        return self.segment_path / "output"

if __name__ == "__main__":
    # todo move to unit tests

    @cast_str_args_to_path
    def try_decorator(arg1, arg2, my_arg=None):
        print(repr(arg1))
        print(repr(arg2))
        print(repr(my_arg))

    try_decorator("hello", "~/foo", my_arg="~/bar/baz")
