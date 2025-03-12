FROM debian:bookworm-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Create a new empty project for caching dependencies
WORKDIR /usr/src/app
RUN cargo new --bin dynamodbbench
WORKDIR /usr/src/app/dynamodbbench

# Copy only the Cargo.toml and Cargo.lock files to cache dependencies
COPY Cargo.toml Cargo.lock* ./

# Build the dependencies (dummy build)
RUN cargo build --release
# Remove the dummy src directory and built artifacts but keep the downloaded dependencies
RUN rm -rf src target/release/deps/dynamodbbench*

# Copy the actual source code
COPY src ./src/

# Build the application with cached dependencies
RUN cargo build --release

# Create a minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/app/dynamodbbench/target/release/dynamodbbench /usr/local/bin/

# Set the entrypoint
ENTRYPOINT ["dynamodbbench"]