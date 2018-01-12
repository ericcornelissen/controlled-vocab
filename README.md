# controlled-vocab

A simple multi-threaded Python CLI tool to created a controlled vocabulary.

### Usage

To convert a single input file into a single output file:

```bash
$ python controlled-vocab.py --input input.txt --output output.txt
```

To convert multiple files into multiple output files:

```bash
$ python controlled-vocab.py --input input_a.txt input_b.txt --output output_a.txt output_b.txt
```

To convert multiple files and fuse the result into a single output file:

```bash
$ python controlled-vocab.py -f --input input_a.txt input_b.txt --output output.txt
```

To convert some input with case sensitivity:

```bash
$ python controlled-vocab.py --input input.txt --output output.txt --case-sensitive
```

To convert some input and keep the mapping:

```bash
$ python controlled-vocab.py --input input-a.txt --output output.txt mapping.json
```

To convert some other input using that mapping:

```bash
$ python controlled-vocab.py --input input-b.txt --mapping mapping.json
```

To put it all together: convert multiple input files fusing the result into a single file using an existing mapping and updating it after this conversion, with case sensitivity:

```bash
$ python controlled-vocab.py -f -c --i input-a.txt input-b.txt -o output.txt mapping.json -m mapping.json
```
