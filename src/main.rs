use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time;

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
    num_queries: usize,

    /// QPS (queries per second) limit
    #[clap(long, default_value = "10")]
    qps: u32,

    /// AWS region
    #[clap(short, long, default_value = "us-west-2")]
    region: String,

    /// Parallelism level (number of concurrent queries)
    #[clap(short = 'k', long, default_value = "1")]
    parallelism: usize,
    
    /// Number of warmup queries to run before the benchmark (to eliminate cold-start effects)
    #[clap(short = 'w', long, default_value = "10")]
    warmup_queries: usize,
}

fn make_query(client: &Client, args: &Args) -> QueryFluentBuilder {
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

    client
        .query()
        .table_name(&args.table)
        .key_condition_expression(key_cond_expr)
        .set_expression_attribute_names(Some(expr_attr_names))
        .set_expression_attribute_values(Some(expr_attr_values))
}

fn quantile_ms(sorted_durations: &[Duration], quantile: f64) -> f64 {
    sorted_durations[((sorted_durations.len() as f64 * quantile).ceil() as usize).max(1) - 1].as_micros() as f64 / 1000.0
}

#[tokio::main]
async fn main() -> () {
    let args = Args::parse();

    // Initialize AWS SDK
    let config = aws_config::from_env()
        .region(aws_sdk_dynamodb::config::Region::new(args.region.clone()))
        .load()
        .await;
    let client = Client::new(&config);
    let query = make_query(&client, &args);

    println!("Starting benchmark with {} queries at {} QPS with parallelism of {}", 
        args.num_queries, args.qps, args.parallelism);
    println!("Table: {}, Partition Key: {} = {}", 
        args.table, args.partition_key, args.partition_value);
    println!("Sort Key: {}, Range: {} to {}", 
        args.sort_key, args.sort_start, args.sort_end);

    let (error_sender, errors) = std::sync::mpsc::channel();
    let semaphore = Arc::new(Semaphore::new(args.parallelism));
    println!("Starting {} warmup queries", args.warmup_queries);
    let start = time::Instant::now();
    let mut interval = time::interval_at(start, Duration::from_secs_f64(1.0 / args.qps as f64));
    for _ in 0..args.warmup_queries {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let query = query.clone();
        let error_sender = error_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = query.send().await {
                error_sender.send(e).unwrap();
            };
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(args.parallelism as u32).await.unwrap();
    println!("Completed warmups in {}s", start.elapsed().as_secs_f64());

    let (sender, durations) = std::sync::mpsc::sync_channel(args.num_queries);

    let start = time::Instant::now();
    interval.reset_at(start);

    for _ in 0..args.num_queries {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let query = query.clone();
        let sender = sender.clone();
        let error_sender = error_sender.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let resp = query.send().await;
            sender.try_send(start.elapsed()).unwrap();
            if let Err(e) = resp {
                error_sender.send(e).unwrap();
            };
            drop(permit);
        });
    }
    drop(sender);
    drop(error_sender);

    // waits for all tasks to complete
    let _ = semaphore.acquire_many(args.parallelism as u32).await.unwrap();
    let total_duration = start.elapsed();

    let errors: Vec<_> = errors.into_iter().collect();
    if !errors.is_empty() {
        println!("\nReceived {} errors:", errors.len());
        for error in errors {
            println!("{:?}", error);
        }
    }

    let mut durations: Vec<Duration> = durations.into_iter().collect();
    durations.sort();

    println!("\nLatency Statistics (milliseconds):");
    println!("Min: {:.3}", quantile_ms(&durations, 0.0));
    println!("Max: {:.3}", quantile_ms(&durations, 1.0));
    // println!("Mean: {:.3}", quantile_ms() );
    // println!("Stddev: {:.3}", hist.stdev() / 1000.0);
    println!("\nPercentiles:");
    println!("p50: {:.3}", quantile_ms(&durations, 0.5));
    println!("p90: {:.3}", quantile_ms(&durations, 0.9));
    println!("p95: {:.3}", quantile_ms(&durations, 0.95));
    println!("p99: {:.3}", quantile_ms(&durations, 0.99));
    println!("p99.9: {:.3}", quantile_ms(&durations, 0.999));
    println!("\nThroughput: {:.1} queries/second", 
        args.num_queries as f64 / total_duration.as_secs_f64());
}