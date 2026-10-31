# 1. Ray basics

Prerequisite: finish [repository setup](../README.md). Keep your virtual
environment active and run commands from the repository root. No GPU or network
access is needed after installation.

A **driver** is the Python process running your script. Ray schedules **tasks**
and **actors** in worker processes. A local runtime lets you learn these APIs on
one machine before moving to a cluster. Each lesson starts two logical CPUs and
shuts down its runtime in a `finally` block.

## Step 1: a remote function

Read [01_remote_tasks.py](01_remote_tasks.py), then run:

```bash
python 01_basics/01_remote_tasks.py
```

`@ray.remote` makes `square` a remote task. `square.remote(7)` submits it and
returns an `ObjectRef`; `ray.get()` retrieves the value, waiting if necessary.
Expect `Reference type: ObjectRef` and `square(7) = 49`.

Try changing 7 to 9. Before running, predict which line prints a reference type
and which line prints 81. Read the [task guide](https://docs.ray.io/en/latest/ray-core/tasks.html).

## Step 2: submit independent work together

```bash
python 01_basics/02_parallel_tasks.py
```

Read [02_parallel_tasks.py](02_parallel_tasks.py). Four tasks are submitted before
one `ray.get(pending)` call. Expect `Squares: [0, 1, 4, 9]` and worker PIDs, whose
values vary. Each task requests one CPU; two can be scheduled at once in this
runtime. `ray.get(list_of_refs)` preserves input order even if completion order
differs.

Try moving `ray.get()` into the submission loop, then restore the original code.
Explain why waiting after each submission prevents overlap. The sleep simulates
work; Ray startup and scheduling overhead can outweigh this tiny example, so it
is not a speed benchmark. Logical CPU requests guide scheduling; they do not
limit the operating system's CPU usage.

## Step 3: shared inputs and task dependencies

```bash
python 01_basics/03_shared_objects.py
```

Read [03_shared_objects.py](03_shared_objects.py). `ray.put()` stores a list once
and returns its reference. Both `total` tasks receive that reference. A top-level
reference argument is resolved to its value before a task executes. Passing the
first task's reference to `scale` creates a dependency without fetching the
intermediate value into the driver.

Expect `Shared totals: [10, 10]` and `Dependent result: 100`. Change the input to
`[2, 4, 6]` and predict both outputs. Objects are immutable shared inputs, not
shared mutable Python variables. References keep objects alive; large objects
can spill to disk. On multiple nodes, Ray may transfer or cache copies near
workers. Read the [objects guide](https://docs.ray.io/en/latest/ray-core/objects.html).

## Step 4: stateful actors

```bash
python 01_basics/04_stateful_actors.py
```

Read [04_stateful_actors.py](04_stateful_actors.py). `Counter.remote()` creates a
worker with its own state. The first actor processes three increments; the second
starts with independent state. Expect `First counter: [1, 2, 3]; second counter: 0`.
These synchronous methods execute sequentially within each actor for calls from
this driver. Chapter 2 introduces async actors and their different concurrency
behavior.

Try `first.increment.remote(5)` and predict how later counts change. Explain why
a task with a local variable would not provide the same persistent counter.
Read the [actors guide](https://docs.ray.io/en/latest/ray-core/actors.html).

## Step 5: partial results and failures

```bash
python 01_basics/05_wait_and_errors.py
```

Read [05_wait_and_errors.py](05_wait_and_errors.py). `ray.wait()` returns ready
and remaining references. It does not retrieve values or raise the task's
exception. `ray.get()` retrieves each ready value and raises `RayTaskError` for
the deliberate division by zero. One task failing does not prevent retrieving
the others.

Expect an error-handled message and
`Successful values (sorted): [4.0, 6.0]; failed tasks: 1`. Ray may also log the
worker exception. Completion order can vary. Change the zero denominator to 4;
predict three successful values and no errors. The lesson bounds each wait so a
stalled task does not leave the loop waiting forever.

## Check your understanding

Before continuing, explain when to use a task versus an actor, why submitting
before waiting matters, and how an `ObjectRef` differs from its value. Then
continue to [2. Workflows](../02_workflows/README.md).
