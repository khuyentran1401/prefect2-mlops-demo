from functools import partial, wraps

from prefect import flow, task


def custom_task(func=None, **task_init_kwargs):
    if func is None:
        return partial(custom_task, **task_init_kwargs)

    @wraps(func)
    def safe_func(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            print(f"The returned value of {func.__name__} is {res}.")
            return res
        except Exception as e:
            print(e)

    safe_func.__name__ = func.__name__
    return task(safe_func, **task_init_kwargs)


@custom_task
def my_task(x):
    return x


@flow
def my_flow():
    my_task(2)


my_flow()
