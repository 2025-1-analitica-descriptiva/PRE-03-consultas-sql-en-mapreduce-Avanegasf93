"""Map/Reduce Immplementation"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import os

def _load_input(input_directory):
    """Load all files from the input directory and return a sequence of (filename, line) tuples."""
    sequence = []
    files = glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files) as f:
        for line in f:
            sequence.append((fileinput.filename(), line))
    return sequence

def _shuffle_and_sort(sequence):
    """Sort the sequence by filename."""
    return sorted(sequence, key=lambda x: x[0])

def _create_output_directory(output_directory):
    """Create or clean the output directory."""
    if os.path.exists(output_directory):
        for file in glob.glob(f"{output_directory}/*"):
            os.remove(file)
        os.rmdir(output_directory)
    os.mkdir(output_directory)

def _save_output(output_directory, sequence):
    """Save the output sequence to a file."""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")

def _create_marker(output_directory):
    """Create a success marker file."""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

def run_mapreduce_job(mapper, reducer, input_directory, output_directory):
    """Execute the MapReduce job with the provided mapper and reducer functions."""
    sequence = _load_input(input_directory)
    sequence = mapper(sequence)
    sequence = _shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    _create_output_directory(output_directory)
    _save_output(output_directory, sequence)
    _create_marker(output_directory)