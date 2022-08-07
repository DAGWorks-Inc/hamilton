# Hamilton + Async

This is currently an experimental feature, allowing one to run a hamilton DAG composed (entirely or partially) of async functions.

## How to use

See the [example](fastapi.py) for the example. The difference from a normal driver is two-fold:

1. You call it using the `AsyncDriver` rather than the standard driver
2. `raw_execute`, and `execute` are both coroutines, meaning they should be called with `await`.
3. It allows for coroutines as inputs -- they'll get properly awaited

To run the example, make sure to install `requirements.txt`.

Then run `uvicorn fastapi_example:app` in one terminal.

Then curl with:

```bash
curl -X 'POST' \
  'http://localhost:8000/execute' \
  -H 'accept: application/json' \
  -d ''
```

You should get the following result:

```json
{"pipeline":{"computation1":false,"computation2":true}}
```


## How it works

Behind the scenes, we create a [GraphAdapter](../../hamilton/experimental/h_async.py)
that turns every function into a coroutine. The function graph then executes, solely creating
more coroutines, that are evaluated at the end. Thus no computation is done until a final node
is awaited.

Any node inputs are awaited on prior to node computation if they are coroutines.

This actually caches the coroutine outputs so we calculate a node once, and stay far away from
python's issues around asking an already-computed coroutine its result (which is near impossible).

## Caveats

1. This will break in certain cases when decorating an async function (E.G. with `extract_outputs`).
This is because the output of that function is never awaited during delegation. We are looking into ways to fix this,
but for now be careful. We will at least be adding validation so the errors are clearer.
2. Performance *should* be close to optimal but we have not benchmarked. We welcome contributions
3. Currently there is the potential for a memory leak. This is due to the coroutine cache we use,
which we really shouldn't need to. That said, you'll want to create a driver on a per-request basis (for now)
## Next steps

We want feedback! We can determine how to make this part of the core API once we get userse who are happy,
so have some fun!

Fixing the caveats will be the first order of business, and adding validations when things won't work.
