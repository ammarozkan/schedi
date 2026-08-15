#!/bin/bash

make all -B

TIMEFORMAT='%R'

testcount=${1:-10}

pcts=()

for i in $(seq 1 "$testcount"); do
	echo "Test $i"

	mono=$( { time ./bin/aparalleljob_mono >/dev/null; } 2>&1 )
	poly=$( { time ./bin/aparalleljob >/dev/null; } 2>&1 )

	pct=$(python3 -c "print(100*$poly/$mono - 100)")
	pcts+=("$pct")
	echo "Mono: ${mono}s  Parallel: ${poly}s  Change: ${pct}%"
done

echo "${pcts[*]}"

echo "---"
python3 <<EOF
import sys, statistics
_arr = "${pcts[*]}".split(' ')
arr = [float(x) for x in _arr]
mean = sum(arr)/len(arr)
stdev = statistics.stdev(arr)
print(f"{len(arr)} samples")
print("mean: {:.6f}".format(mean))
print("stdev (sample): {:.6f}".format(stdev))
print(arr)
EOF
