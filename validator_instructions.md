
Lets write a script to valdidate scraped data.

We are working with insane amounts of data. Each of these files alone is going to be potentially terabytes large. Larger than is 
possible to store in memory. You cannot create a set of IDs and not run out of memory either. Do not attempt. The only way for us to 
process files is to stream them.

# Part 1
The script should take a folder of a mix of jsonl or jsonl.gz files along with the `private.txt` and `public.txt`.
First, validate that all these files are present. That there are some number of `.jsonl`/`.jsonl.gz` files, plus private and public.
Do not include anything in subfolders, only base level in the cwd.

# Part 2
If the files do not already exist, sort the public and private lists and write them back sorted to disk in the same folder as
`public.packed` and `private.packed` as uint32s instead of as strings in the file. You can use the `sort` command to do the sorting
(sort numerically) and copy to a temp file in /tmp, then read back the files and convert to uint32s and delete the temp file.

# Part 3
If `skipped.txt` does not already exist, stream the packed files back together and write a `skipped.txt` that contains all the IDs
starting from 1 that are not present in either file. Then print the amount of IDs that were skipped.

# Part 4
For the next step, check if `sorted.flag` exists. If not, skip this step.

We shall read the `.jsonl`/`.jsonl.gz` files. Use the gzip pip package. Stream in the lines, and write to a new folder, `sorted_outputs/`,
`<start>_<end>.jsonl` files with a range of a hundred thousand IDs each. Append to them (automatically creating them) as you encounter
new IDs in that range.

Then, sort each jsonl file numerically by `"id"` (they will be stored like `{"id": "3", ...}`), and store to `<start>_<end>_sorted.jsonl`.
Make sure that each line json decodes properly. Drop the ones that do not. Find a way to sort without reading the entire
file in at the same time.

Afterwards, delete the unsorted files and `touch sorted.flag`.

# Part 4

Print out the number of lines which failed validation. If any did

# Part 5
For this step, check if `missing.txt` exists.

After doing this for the files, stream in the sorted files the sorted files and compare against `skipped.txt`. Report any records
that are missing in a new file, `missing.txt`. Then print out the number of IDs that were missing.
