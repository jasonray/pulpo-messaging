truncate -s 0 /tmp/kessel/kessel.log

python3 runner.py >> /tmp/kessel/kessel.log 2>&1 &
python3 runner.py >> /tmp/kessel/kessel.log 2>&1 &

