
Let's use reference.py as, you guessed it, a reference, to build a server.py and a worker.py.

```sh
# To launch worker:
./worker.py --server <server IP or loopback if not present>

# To launch server:
./server.py --output <folder name or output/ if not present> --start-id <int or 1 if not present> --end-id <int or 1 billion if not present>
```

The server should read the files in --outputs, if present, and send lists of IDs to clients to be scraped. The clients then return lines back one at a time to the server to be
immediately appended to the outputs.

The outputs folder should contain three files. One that contains the results for scraping public works (append to results.jsonl), a public.txt which contains a newline separated list
of IDs that have been successfully scraped, and a private.txt that contains a list of IDs that have been skipped because they are private (they returned a 404).

By reading the two public.txt and private.txt output files, the server should be able to resume from anywhere. Do not try to read the jsonl file, it will likely be terabytes. Don't
use the progress.json format at all, it is bad. Use the same format for the jsonl file as reference.py, except instead of concatenating the chapters, let's just do an array of
chapters of form {"title": title, "text": chapter_text}. If there are no chapters, that should not be an error.

Rate limited requests (429) should not fail after any number of retries. We should just sleep for `int(response.headers['retry-after'])` or 5 minutes (300 seconds) before resuming.
Don't add a timeout, we can expect AO3 to be well behaved.

The reference impl uses aiohttp. Don't use aiohttp. Let's do everything synchronously on the client. The server will presumably handle requests async in whatever fashion that FastAPI
chooses, but we shouldn't try to parallelize/make `async` anything else. Writes can be We may waste some time reading and writing files, that's fine. We are bound by the rate limit on
the clients, not anything else.

Unlike reference.py, do not excessively use classes. Try to not use the `async` keyword at all. This is important. But if FastAPI requires it you may do so once.
