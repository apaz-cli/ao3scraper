
# AO3 Scraper

There are other AO3 scrapers out there. This one is better. More reliable. More robust. I have scaled this to hundreds of machines. It works great.

## Local usage:
```sh
# Classic python install instructions.
# Run this wherever you want to run the server, worker, or monitor.
python -m venv .venv/
. .venv/bin/activate
pip install -r requirements.txt
```
```sh
./server.py
./worker.py # In another terminal
./monitor.py  # In yet another terminal to see your progress
```

This will run everything on your local machine. Given enough time it will download all of AO3 to the `output/` folder in `output/results.jsonl`. The other files are there to keep track of progress. The server can resume from any point, just ctrl+C and re-run.

But this local setup will be agonizingly slow. You will want to run workers on multiple machines. Probably hundreds, as I have done. It sounds extreme, but this is the most reasonable solution to downloading all of AO3. We'll get to exactly how to do that later. For now, here are the basic commands.


## Multi-machine usage:
```sh
# On a machine with a public IP address with SSH and ports forwarded to it. I recommend getting a VPS with a bunch of storage.
./server.py
```
```sh
# On each worker machine
./worker.py --server <server-ip-address>
```
```sh
# On local machine to see progress
./monitor.py --server <server-ip-address>
```

No need to append the port info (`:8000`). It is assumed to be 8000 by default. If you want to change the port, pass --port `<port-number>` to each of the programs. I recommend looking at `--help` for each program to see options, especially `server.py`.


## Grabbing your files
So, now the data is flowing in. A lot of data. You will want to keep an eye on your disk usage with the monitoring script. Periodically, you may want to grab the data from your server as it's downloading to free up space. Here's how you do that.


* 🟠 means "Run this on your local machine."
* 🔵 means "Run this on the server."

```sh
# 🔵 Rename the results.jsonl file to something else. The server will create
# a new results.jsonl file to append to automatically.
# Be sure that what you rename it to is on the same filesystem, otherwise the rename
# will not actually be atomic. Two seconds should be plenty of time for the server to
# close the file and complete any pending writes. Then you can move it wherever you want.
mv output/results.jsonl output/results_0.jsonl && sleep 2 && mv output/results_0.jsonl ~/results_0.jsonl

# 🔵 Compress the file to speed up transfer (optional). This may take a while.
gzip ~/results_0.jsonl

# 🟠 Use rsync to securely copy the file to your local machine. Your command will of course
# vary based on the IP, file name, etc, but it should look like this. This too may take a while.
rsync -v -e "ssh -p 22 -i private_key.pem" ubuntu@204.55.27.121:~/results_0.jsonl.gz results_0.jsonl

# 🟠 Decompress the file (if you compressed it earlier).
gunzip results_0.jsonl.gz

# 🔵 Remove the file from the server to free up space.
rm ~/results_0.jsonl.gz
```

## Shutdown
I tried my absolute best to ensure that there's no data loss/inconsistency possible when you randomly kill the server. Just ctrl+C does the same thing, but if you want to kill it from elsewhere you can use shutdown.py. See `./shutdown.py --help`.


## Modal Swarm
[Modal.com](https://modal.com/) is a GPU neocloud. I use them for writing and profiling GPU kernels, which is the normal use case. But they also have tiny CPU VMs, and a nice SDK. I know some of the people who run it, and it's very easy to use, and they have a generous free tier. So it's what I used to scale up. The free tier isn't going to get you all of AO3, but it can probably get you through like 6 million IDs or so. 

If you want to use Modal, you can go to their [Getting Started](https://modal.com/apps) page, make an account, and then run:

```
pip install -r swarm_requirements.txt
python -m modal setup
```

The modal setup command will bring you back to the web browser to sign in to the account that you made. Then you should just be able to start the server somewhere that has a public IP (I used a CPU node from Prime Intellect) and run:

```
./swarm.py <server-ip>
```

And you should see an insane amount of data flowing in.


Godspeed, and happy scraping. Enjoy your smut, you weirdo.
