
Lets write a script to valdidate the data we scrape.

# Part 1
It should take a folder of a mix of jsonl or jsonl.gz files along with the `private.txt` and `public.txt`.
First, validate that all these files are present. That there are some number of `.jsonl`/`.jsonl.gz` files, plus private and public.
Do not include anything in subfolders, only base level in the cwd.

Each of these files alone is going to be larger than is possible to store in memory. Many many gigabytes. You cannot create a
set of IDs and not run out of memory either. Do not attempt.

# Part 2
If the files do not already exist, sort the public and private lists and write them back sorted to disk in the same folder as
`public.packed` and `private.packed` as uint32s instead of as strings in the file. You can use the `sort` command to do the sorting
(sort numerically) and copy to a temp file in /tmp, then read back the files and convert to uint32s and delete the temp file.

# Part 3
If `skipped.txt` does not already exist, stream the packed files back together and write a `skipped.txt` that contains all the IDs
starting from 1 that are not present in either file. Then print the amount of IDs that were skipped.

# Part 4
For the next step, check if `sorted.flag` exists. If not, skip this step.

we shall read the `.jsonl`/`.jsonl.gz` files. Stream in the lines, and write to a new folder, `sorted_outputs/`,
`<start>_<end>.jsonl` files with a range of a hundred thousand IDs each. Append to them (automatically creating them) as you encounter
new IDs in that range.

Then, sort each jsonl file numerically by ID (they will be stored like `{"id": "3", ...}`), and store to `<start>_<end>_sorted.jsonl`.
The `"id"` tag always starts the line, so no need to actually decode the json here. Find a way to sort without reading the entire
file in at the same time.

Afterwards, delete the unsorted files and `touch sorted.flag`.

# Part 5
For this step, check if `missing.txt` exists.

After doing this for the files, stream in the sorted files the sorted files and compare against `skipped.txt`. Report any records
that are missing in a new file, `missing.txt`. Then print out the number of IDs that were missing.
