use anyhow::{Context, Result};
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use clap::Parser;
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::{task, time};

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about = "DynamoDB range query latency benchmark")]
struct Args {
    /// DynamoDB table name
    #[clap(short, long)]
    table: String,

    /// Partition key name
    #[clap(short = 'p', long)]
    partition_key: String,

    /// Sort key name
    #[clap(short = 's', long)]
    sort_key: String,

    /// Partition key value
    #[clap(short = 'P', long)]
    partition_value: String,

    /// Sort key start value (for range query)
    #[clap(short = 'S', long)]
    sort_start: String,

    /// Sort key end value (for range query)
    #[clap(short = 'E', long)]
    sort_end: String,

    /// Number of query operations to perform
    #[clap(short, long, default_value = "100")]
    num_queries: u32,

    /// QPS (queries per second) limit
    #[clap(long, default_value = "10")]
    qps: u32,

    /// AWS region
    #[clap(short, long, default_value = "us-west-2")]
    region: String,

    /// Parallelism level (number of concurrent queries)
    #[clap(short = 'k', long, default_value = "1")]
    parallelism: u32,
    
    /// Number of warmup queries to run before the benchmark (to eliminate cold-start effects)
    #[clap(short = 'w', long, default_value = "10")]
    warmup_queries: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // Initialize AWS SDK
    let config = aws_config::from_env()
        .region(aws_sdk_dynamodb::config::Region::new(args.region.clone()))
        .load()
        .await;
    let client = Arc::new(Client::new(&config));

    // Calculate the delay between batches to maintain QPS across all parallel queries
    let effective_qps = if args.qps > 0 { args.qps } else { 1000 }; // Default to high QPS if not specified
    let batch_size = args.parallelism.max(1);
    let delay = if effective_qps > 0 {
        Duration::from_secs_f64(batch_size as f64 / effective_qps as f64)
    } else {
        Duration::from_secs(0)
    };

    // Create a shared histogram to record latencies (in microseconds)
    let histogram = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()
    ));

    // Calculate how many batches we need to run
    let total_queries = args.num_queries;
    let complete_batches = total_queries / batch_size;
    let remainder = total_queries % batch_size;
    let total_batches = if remainder > 0 { complete_batches + 1 } else { complete_batches };

    println!("Starting benchmark with {} queries at {} QPS with parallelism of {}", 
        args.num_queries, effective_qps, args.parallelism);
    println!("Table: {}, Partition Key: {} = {}", 
        args.table, args.partition_key, args.partition_value);
    println!("Sort Key: {}, Range: {} to {}", 
        args.sort_key, args.sort_start, args.sort_end);
    println!("Running in {} batches with up to {} concurrent queries per batch", 
        total_batches, batch_size);
    
    // Run warmup queries to eliminate cold-start effects
    if args.warmup_queries > 0 {
        println!("Running {} warmup queries...", args.warmup_queries);
        let warmup_start = Instant::now();
        
        // Use the same parallelism level for warmup as for the benchmark
        let warmup_batch_size = args.parallelism.max(1);
        let warmup_batches = (args.warmup_queries + warmup_batch_size - 1) / warmup_batch_size;
        
        for batch in 0..warmup_batches {
            let mut handles = Vec::new();
            
            let current_batch_size = if batch == warmup_batches - 1 && args.warmup_queries % warmup_batch_size != 0 {
                args.warmup_queries % warmup_batch_size
            } else {
                warmup_batch_size
            };
            
            for _ in 0..current_batch_size {
                let client_clone = Arc::clone(&client);
                let args_clone = args.clone();
                
                let handle = task::spawn(async move {
                    let start = Instant::now();
                    match query_range(&client_clone, &args_clone).await {
                        Ok(_) => {
                            let duration = start.elapsed();
                            tracing::debug!("Warmup query completed in {} ms", duration.as_millis());
                            Ok(duration)
                        },
                        Err(e) => Err(e),
                    }
                });
                
                handles.push(handle);
            }
            
            for handle in handles {
                if let Ok(result) = handle.await {
                    if let Err(e) = result {
                        eprintln!("Warmup query error: {}", e);
                    }
                }
            }
        }
        
        println!("Warmup completed in {} ms, starting benchmark...", 
            warmup_start.elapsed().as_millis());
    }
    
    // Shared counter for progress reporting
    let completed_queries = Arc::new(Mutex::new(0u32));
    
    // Run the benchmark in batches with parallelism
    for batch in 0..total_batches {
        let batch_start = Instant::now();
        let mut handles = Vec::new();
        
        // Calculate batch size (might be smaller for the last batch)
        let current_batch_size = if batch == complete_batches && remainder > 0 {
            remainder
        } else {
            batch_size
        };
        
        // Launch parallel queries for this batch
        for _ in 0..current_batch_size {
            let client_clone = Arc::clone(&client);
            let args_clone = args.clone();
            let histogram_clone = Arc::clone(&histogram);
            let completed_clone = Arc::clone(&completed_queries);
            
            let handle = task::spawn(async move {
                // Perform the query and measure latency
                let start = Instant::now();
                match query_range(&client_clone, &args_clone).await {
                    Ok(_) => {
                        let latency = start.elapsed();
                        let latency_us = latency.as_micros() as u64;
                        
                        // Record the latency in our shared histogram
                        let mut hist = histogram_clone.lock().await;
                        let _ = hist.record(latency_us);
                        // Release the lock
                        drop(hist);
                        
                        // Update completed count
                        let mut completed = completed_clone.lock().await;
                        *completed += 1;
                        if *completed % 10 == 0 || *completed == total_queries {
                            println!("Completed {}/{} queries", *completed, total_queries);
                        }
                        // Release the lock
                        drop(completed);
                        
                        Ok(latency)
                    },
                    Err(e) => Err(e),
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all queries in this batch to complete
        for handle in handles {
            if let Ok(result) = handle.await {
                if let Err(e) = result {
                    eprintln!("Query error: {}", e);
                }
            }
        }
        
        // Sleep to maintain the target QPS
        let batch_duration = batch_start.elapsed();
        if batch_duration < delay {
            time::sleep(delay - batch_duration).await;
        }
    }

    // Print the results
    let hist = histogram.lock().await;
    println!("\nLatency Statistics (milliseconds):");
    println!("Min: {:.3}", hist.min() as f64 / 1000.0);
    println!("Max: {:.3}", hist.max() as f64 / 1000.0);
    println!("Mean: {:.3}", hist.mean() / 1000.0);
    println!("Stddev: {:.3}", hist.stdev() / 1000.0);
    println!("\nPercentiles:");
    println!("p50: {:.3}", hist.value_at_percentile(50.0) as f64 / 1000.0);
    println!("p90: {:.3}", hist.value_at_percentile(90.0) as f64 / 1000.0);
    println!("p95: {:.3}", hist.value_at_percentile(95.0) as f64 / 1000.0);
    println!("p99: {:.3}", hist.value_at_percentile(99.0) as f64 / 1000.0);
    println!("p99.9: {:.3}", hist.value_at_percentile(99.9) as f64 / 1000.0);
    println!("\nThroughput: {:.1} queries/second", 
        args.num_queries as f64 / hist.max() as f64 * 1_000_000.0);

    Ok(())
}

async fn query_range(client: &Client, args: &Args) -> Result<()> {
    // Set up query expression attribute values
    let mut expr_attr_values = HashMap::new();
    expr_attr_values.insert(
        ":pk".to_string(),
        AttributeValue::S(args.partition_value.clone()),
    );
    expr_attr_values.insert(
        ":start".to_string(),
        AttributeValue::S(args.sort_start.clone()),
    );
    expr_attr_values.insert(
        ":end".to_string(),
        AttributeValue::S(args.sort_end.clone()),
    );

    let mut expr_attr_names = HashMap::new();
    expr_attr_names.insert(format!("#{}", args.partition_key), args.partition_key.clone());
    expr_attr_names.insert(format!("#{}", args.sort_key), args.sort_key.clone());

    // Build key condition expression
    let key_cond_expr = format!(
        "#{} = :pk AND #{} BETWEEN :start AND :end",
        args.partition_key, args.sort_key
    );

    let resp = client
        .query()
        .table_name(&args.table)
        .key_condition_expression(key_cond_expr)
        .set_expression_attribute_names(Some(expr_attr_names))
        .set_expression_attribute_values(Some(expr_attr_values))
        .send()
        .await
        .context("Failed to execute DynamoDB query")?;

    // For benchmarking, we care about timing, not about processing the data
    // Just report the count of items
    let count = resp.count;
    tracing::debug!("Query returned {} items", count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    use std::collections::HashMap;
    
    // Mock client for testing
    struct MockClient {
        items_count: i32,
        delay: Duration,
    }
    
    impl MockClient {
        fn new(items_count: i32, delay_ms: u64) -> Self {
            Self {
                items_count,
                delay: Duration::from_millis(delay_ms),
            }
        }
        
        async fn query(&self) -> Result<QueryOutput> {
            // Simulate network delay
            time::sleep(self.delay).await;
            
            // Create mock response
            let builder = QueryOutput::builder().count(self.items_count);
            
            Ok(builder.build())
        }
    }
    
    #[tokio::test]
    async fn test_parallel_queries_are_faster() {
        // Create simple test params
        let parallelism = 4;
        let num_queries = 20;
        let delay_ms = 50; // Each query takes 50ms
        
        // Test with parallelism=1
        let start = Instant::now();
        run_test_queries(1, num_queries, delay_ms).await;
        let serial_duration = start.elapsed();
        
        // Test with parallelism=4
        let start = Instant::now();
        run_test_queries(parallelism, num_queries, delay_ms).await;
        let parallel_duration = start.elapsed();
        
        println!("Serial duration: {:?}", serial_duration);
        println!("Parallel duration with {} threads: {:?}", parallelism, parallel_duration);
        
        // Parallel should be significantly faster (at least 2x)
        assert!(parallel_duration < serial_duration / 2);
    }
    
    async fn run_test_queries(parallelism: u32, num_queries: u32, delay_ms: u64) {
        // Create a counter to track completed queries
        let completed = Arc::new(Mutex::new(0u32));
        
        // Calculate batch sizes
        let batch_size = parallelism.max(1);
        let complete_batches = num_queries / batch_size;
        let remainder = num_queries % batch_size;
        let total_batches = if remainder > 0 { complete_batches + 1 } else { complete_batches };
        
        for batch in 0..total_batches {
            let mut handles = Vec::new();
            
            // Calculate batch size (might be smaller for the last batch)
            let current_batch_size = if batch == complete_batches && remainder > 0 {
                remainder
            } else {
                batch_size
            };
            
            // Launch parallel queries for this batch
            for _ in 0..current_batch_size {
                let completed_clone = Arc::clone(&completed);
                let mock_client = MockClient::new(10, delay_ms);
                
                let handle = task::spawn(async move {
                    let _ = mock_client.query().await;
                    
                    // Update completed count
                    let mut completed = completed_clone.lock().await;
                    *completed += 1;
                });
                
                handles.push(handle);
            }
            
            // Wait for all queries in this batch to complete
            for handle in handles {
                let _ = handle.await;
            }
        }
    }
}
