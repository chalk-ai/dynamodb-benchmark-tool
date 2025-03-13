use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use clap::{Subcommand, Parser, Args};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time;

#[derive(Parser)]
#[command(author, version, about = "DynamoDB range query latency benchmark")]
struct Cli {
    /// DynamoDB endpoint url
    #[arg(short = 'u', long)]
    endpoint_url: Option<String>,

    /// DynamoDB table name
    #[arg(short, long)]
    table: String,

    /// AWS region
    #[arg(short, long)]
    region: String,

    /// Partition key name
    #[arg(short = 'p', long, default_value = "__id__")]
    partition_key: String,

    /// Sort key name
    #[arg(short = 's', long, default_value = "__ns__")]
    sort_key: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Bench(BenchArgs),
    ShowMapping {
        /// Chalk environment name
        #[arg(short, long)]
        environment: String
    }
}

#[derive(Args, Debug)]
struct BenchArgs {
    /// Partition key value
    #[arg(short = 'P', long)]
    partition_value: Vec<String>,

    /// Sort key start value (for range query)
    #[arg(short = 'S', long)]
    sort_start: Option<String>,

    /// Sort key end value (for range query)
    #[arg(short = 'E', long)]
    sort_end: Option<String>,

    /// Number of query operations to perform
    #[arg(short, long, default_value = "100")]
    num_queries: usize,

    /// QPS (queries per second) limit
    #[arg(long, default_value = "10")]
    qps: u32,

    /// Parallelism level (number of concurrent queries)
    #[arg(short = 'k', long, default_value = "1")]
    parallelism: usize,
    
    /// Number of warmup queries to run before the benchmark (to eliminate cold-start effects)
    #[arg(short = 'w', long, default_value = "10")]
    warmup_queries: usize,
}

fn make_query(client: &Client, cli: &Cli, args: &BenchArgs) -> Vec<QueryFluentBuilder> {
    let mut query_without_pkey = client
        .query()
        .table_name(&cli.table)
        .expression_attribute_names("#pk", &cli.partition_key);

    if let Some(start) = &args.sort_start {
        query_without_pkey = query_without_pkey
            .expression_attribute_values(":start", AttributeValue::S(start.clone()))
            .expression_attribute_names("#sk", &cli.sort_key)
    }
    if let Some(end) = &args.sort_end {
        query_without_pkey = query_without_pkey
            .expression_attribute_values(":end", AttributeValue::S(end.clone()))
            .expression_attribute_names("#sk", &cli.sort_key)
    }

    let sort_key_condition = match (args.sort_start.is_some(), args.sort_end.is_some()) {
        (true, true) =>  " AND #sk BETWEEN :start AND :end",
        (true, false) => " AND #sk >= :start",
        (false, true) => " AND #sk <= :end",
        (false, false) => ""
    };

    query_without_pkey = query_without_pkey.key_condition_expression(format!("#pk = :pk{}", sort_key_condition));

    args.partition_value.iter().map(|val| {
        query_without_pkey.clone().expression_attribute_values(":pk", AttributeValue::S(val.clone()))
    }).collect()
}

fn quantile_ms(sorted_durations: &[Duration], quantile: f64) -> f64 {
    sorted_durations[((sorted_durations.len() as f64 * quantile).ceil() as usize).max(1) - 1].as_micros() as f64 / 1000.0
}

#[tokio::main]
async fn main() -> () {
    let cli = Cli::parse();

    // Initialize AWS SDK
    let mut config = aws_config::from_env()
        .region(aws_sdk_dynamodb::config::Region::new(cli.region.clone()));

    if let Some(endpoint_url) = &cli.endpoint_url {
        config = config.endpoint_url(endpoint_url)
    }

    let config = config.load().await;

    let client = Client::new(&config);

    let args = match &cli.command {
        Commands::Bench(args) => args,
        Commands::ShowMapping { environment } => {
            return show_mapping(&client, &cli, &environment).await
        },
    };

    let queries = make_query(&client, &cli, &args);

    println!("Starting benchmark with {} queries at {} QPS with parallelism of {}", 
        args.num_queries, args.qps, args.parallelism);
    println!("Table: {}, Partition Keys: {} = {:?}", 
        cli.table, cli.partition_key, args.partition_value);
    println!("Sort Key: {}, Range: {:?} to {:?}", 
        cli.sort_key, args.sort_start, args.sort_end);

    let (response_sender, responses) = std::sync::mpsc::channel();
    let semaphore = Arc::new(Semaphore::new(args.parallelism));
    println!("Starting {} warmup queries", args.warmup_queries);
    let start = time::Instant::now();
    let mut interval = time::interval_at(start, Duration::from_secs_f64(1.0 / args.qps as f64));
    for i in 0..args.warmup_queries {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let query = queries[i % queries.len()].clone();
        let response_sender = response_sender.clone();
        tokio::spawn(async move {
            let resp = query.send().await;
            drop(permit);
            if let Err(e) = resp {
                response_sender.send(Err(e)).unwrap();
            }
        });
    }

    let _ = semaphore.acquire_many(args.parallelism as u32).await.unwrap();
    println!("Completed warmups in {}s", start.elapsed().as_secs_f64());

    let (sender, durations) = std::sync::mpsc::sync_channel(args.num_queries);

    let start = time::Instant::now();
    interval.reset_at(start);

    for i in 0..args.num_queries {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let query = queries[i % queries.len()].clone();
        let sender = sender.clone();
        let response_sender = response_sender.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let resp = query.send().await;
            sender.try_send(start.elapsed()).unwrap();
            drop(permit);
            response_sender.send(resp.map(|resp| resp.count())).unwrap();
        });
    }
    drop(sender);
    drop(response_sender);

    // waits for all tasks to complete
    let _ = semaphore.acquire_many(args.parallelism as u32).await.unwrap();
    let total_duration = start.elapsed();

    let mut response_stats = HashMap::new();
    for count_or_error in responses {
        if let Err(e) = &count_or_error {
            println!("{:?}", e);
        }
        *response_stats.entry(count_or_error.ok()).or_insert(0) += 1;
    }

    println!("\nResponse stats:");
    for (num_items, num_responses) in response_stats {
        let to_str = num_items.map(|x| format!("{} items", x));
        println!("{}: {} responses", to_str.as_deref().unwrap_or("Error"), num_responses);
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

async fn show_mapping(client: &Client, cli: &Cli, environment: &str) {
    let mut stream = client.query()
        .table_name(&cli.table)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", &cli.partition_key)
        .expression_attribute_values(":pk", AttributeValue::S(format!("__chalk_fqn_mapping__:{}", environment)))
        .into_paginator()
        .send();

    let agg_regex = Regex::new("^(.+):([0-9]+)$").unwrap();

    println!("FQN mappings for {}:", environment);
    let mut table = vec![("Partition key prefix".to_owned(), "Aggregation key".to_owned(), "Bucket duration (ms)".to_owned())];
    while let Some(resp) = stream.next().await {
        let resp = match resp {
            Err(e) => { 
                println!("Encountered query error: {:?}", e);
                continue
            },
            Ok(resp) => resp,
        };
        for item in resp.items() {
            if let (Some(AttributeValue::S(key)), Some(AttributeValue::S(val))) = (item.get(&cli.sort_key), item.get("value")) {
                if let Some(capture) = agg_regex.captures(key) {
                    let agg_on = capture.get(1).unwrap().as_str();
                    let bucket_duration = capture.get(2).unwrap().as_str();
                    table.push((format!("{}:{}:", environment, val), agg_on.to_owned(), bucket_duration.to_owned()));
                }
            } else {
                println!("Malformed item in query: {:?}", item);
            }
        }
    }

    let (w1, w2, w3) = table.iter().fold((0, 0, 0), |(a1, a2, a3), (c1, c2, c3)| (a1.max(c1.len()), a2.max(c2.len()), a3.max(c3.len())));
    let (w1, w2, w3) = (w1 + 4, w2 + 4, w3 + 4);
    for (pkey, agg_on, bucket_duration) in table {
        println!("{pkey:w1$} {agg_on:w2$} {bucket_duration:w3$}");
    }
}