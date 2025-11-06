# MapReduce Word Count

A distributed MapReduce implementation for word counting using Python and RPyC (Remote Python Call).

## Overview

This project implements a distributed word count system using the MapReduce paradigm. It consists of:
- **Coordinator**: Orchestrates the MapReduce workflow (map, shuffle, reduce, aggregate)
- **Workers**: Execute map and reduce tasks in parallel

## Architecture

```
┌─────────────┐
│ Coordinator │
└──────┬──────┘
       │
       ├──────────┬──────────┬──────────┐
       ▼          ▼          ▼          ▼
   Worker-1   Worker-2   Worker-3   Worker-N
```

## Installation

### Clone from GitHub

```bash
git clone https://github.com/Maths-A/CSE239-Fall25.git
cd HW 2
```

### Docker Setup

```bash
cd Repo
docker-compose build
```

## Usage

### Running with Docker Compose

```bash
docker-compose up -d
```

This will start the coordinator and multiple worker containers.

### Running

1. Run the coordinator
You should open the integrated terminal of the coordinator container

```bash
docker exec -it coordinator sh
python coordinator.py [URL] [NUM_MAP_WORKERS] [NUM_REDUCE_WORKERS]
```

**Arguments:**
- `URL` (optional): Dataset URL (default: enwik8.zip)
- `NUM_MAP_WORKERS` (optional): Number of map workers (default: 2)
- `NUM_REDUCE_WORKERS` (optional): Number of reduce workers (default: 2)

**Example:**
```bash
python coordinator.py https://mattmahoney.net/dc/enwik8.zip 4 4
```

This will:
1. Run the coordinator
2. Download and process the dataset
3. Display top 20 most frequent words

## Output Example

```
TOP 20 WORDS BY FREQUENCY

1.      one        : 12345
2.      two        : 11234
3.      three      : 10123
...
```

## Development

### Project Structure
```
Repo/
├── coordinator.py       # Main orchestrator
├── worker.py           # RPC worker service
├── docker-compose.yml  # Container orchestration
├── Dockerfile         # Worker container image
├── README.md          # This file
└── txt/               # Downloaded datasets
```

## Authors

- Mathis AUBERT
