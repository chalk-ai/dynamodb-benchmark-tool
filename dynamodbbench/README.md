# DynamoDB Benchmark Tool

A simple Rust utility to benchmark DynamoDB range query latency.

## Features

- Performs range queries against a DynamoDB table
- Controls query rate (QPS)
- Records percentile latency distribution
- Provides detailed statistics (min, max, mean, p50, p90, p95, p99, p99.9)

## Prerequisites

- Rust and Cargo
- AWS credentials configured (either via environment variables, ~/.aws/credentials, or IAM role)

## Building

```bash
cargo build --release
```

## Cross-Compiling for x86_64 Linux (e.g., EC2)

To cross-compile for x86_64 Linux from macOS:

1. Install the required tools:

```bash
# Install cross-compilation tools (macOS)
brew install FiloSottile/musl-cross/musl-cross
```

2. Add the target:

```bash
rustup target add x86_64-unknown-linux-musl
```

3. Build for the target platform:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

The compiled binary will be at `target/x86_64-unknown-linux-musl/release/dynamodbbench`

4. Transfer the binary to your EC2 instance:

```bash
scp -i your-key.pem target/x86_64-unknown-linux-musl/release/dynamodbbench ec2-user@your-ec2-instance:~/
```

Using the musl target creates a statically linked binary that doesn't depend on system libraries, making it more portable across different Linux distributions.

## Usage

```bash
# Example with optimizations for reducing tail latency
./target/release/dynamodbbench \
  --table my-dynamodb-table \
  --partition-key pk \
  --sort-key sk \
  --partition-value "customer#123" \
  --sort-start "order#2020-01-01" \
  --sort-end "order#2020-12-31" \
  --num-queries 1000 \
  --qps 20 \
  --parallelism 8 \
  --warmup-queries 20 \
  --eventually-consistent \
  --max-retries 2 \
  --timeout-ms 500 \
  --region us-east-1
```

### Command Line Arguments

- `-t, --table`: DynamoDB table name
- `-p, --partition-key`: Partition key name
- `-s, --sort-key`: Sort key name
- `-P, --partition-value`: Partition key value
- `-S, --sort-start`: Sort key start value (for range query)
- `-E, --sort-end`: Sort key end value (for range query)
- `-n, --num-queries`: Number of query operations to perform (default: 100)
- `--qps`: Queries per second limit (default: 10)
- `-r, --region`: AWS region (default: us-west-2)
- `-k, --parallelism`: Number of concurrent queries to run (default: 1)
- `-w, --warmup-queries`: Number of warmup queries to run before the benchmark (default: 10)
- `--eventually-consistent`: Use eventually consistent reads for lower latency (default: false, uses strongly consistent reads)
- `--max-retries`: Maximum number of retry attempts (default: 3)
- `--timeout-ms`: Timeout for each query in milliseconds (default: 0, no timeout)
- `--max-connections`: Maximum connections per host in the connection pool (default: 50)

## Output

The tool will print progress updates during the benchmark and finish with a detailed latency report:

```
Starting benchmark with 100 queries at 10 QPS with parallelism of 4
Table: my-table, Partition Key: pk = customer#123
Sort Key: sk, Range: order#2020-01-01 to order#2020-12-31
Running in 25 batches with up to 4 concurrent queries per batch
Running 10 warmup queries...
Warmup completed in 487 ms, starting benchmark...
Completed 10/100 queries
Completed 20/100 queries
...
Completed 100/100 queries

Latency Statistics (milliseconds):
Min: 32.123
Max: 87.456
Mean: 45.678
Stddev: 12.345

Percentiles:
p50:    44.567
p75:    55.234
p90:    65.432
p95:    75.123
p99:    83.456
p99.9:  87.123
p99.99: 87.456

Latency Ratios (higher values indicate worse tail latency):
p99/p50: 1.87x
p99.9/p50: 1.96x

Performance Summary:
Total Duration: 2.367 seconds
Throughput: 42.3 queries/second
Configuration: parallelism=4, consistency=eventual, max_retries=3
```

## Tips for Reducing Tail Latency

When benchmarking DynamoDB with high parallelism and QPS, you may encounter high tail latency (p99, p99.9). Here are some strategies to mitigate this:

1. **Use Eventually Consistent Reads**: By using the `--eventually-consistent` flag, you can reduce tail latency at the cost of potential stale reads. This can reduce p99 latency significantly.

2. **Optimize Retry Policy**: Excessive retries can contribute to tail latency. Try reducing `--max-retries` to 1 or 2 instead of the default 3.

3. **Set Reasonable Timeouts**: Adding a timeout with `--timeout-ms` can prevent slow queries from affecting your tail latency statistics.

4. **Gradual Parallelism Increase**: Start with low parallelism (e.g., `-k 1`) and gradually increase to find the optimal point before tail latency becomes unacceptable.

5. **Monitor Latency Ratios**: The p99/p50 and p99.9/p50 ratios help you understand the magnitude of your tail latency. Typically, a ratio below 2-3x is considered good.

6. **Understand DynamoDB Limits**: Be aware of your provisioned capacity (for provisioned tables) or burst capacity (for on-demand tables).

7. **Try Different Regions**: Some AWS regions may have lower tail latency for your specific workload.

8. **Tune Connection Pool**: Using `--max-connections` you can adjust the HTTP connection pool size, which can help with high-parallelism workloads.

Example benchmark comparison with different settings:

| Configuration | p50 (ms) | p99 (ms) | p99/p50 ratio | Throughput (QPS) |
|---------------|----------|----------|---------------|------------------|
| Strong consistency, 10 parallel | 42.1 | 134.7 | 3.2x | 230 |
| Eventual consistency, 10 parallel | 35.8 | 87.6 | 2.4x | 265 |
| Eventual consistency, 20 parallel | 38.2 | 152.3 | 4.0x | 310 |
| Eventual consistency, 10 parallel, max retries=1 | 36.5 | 75.2 | 2.1x | 255 |