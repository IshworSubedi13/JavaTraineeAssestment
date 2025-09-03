# JavaTraineeAssestment

A Java-based practice project demonstrating parallel CSV processing and task scheduling.

## Overview

This repository contains two core Java classes:

- **ParallelCsvAggregator**: Reads and processes large CSV files in parallel using multiple threads, then aggregates user statistics.
- **TaskScheduler**: Demonstrates how to schedule recurring or delayed tasks within a Java application.

Additionally, there’s a sample dataset (`sample_5k.csv`) to test and benchmark the CSV-processing capabilities.


## Prerequisites

- Java Development Kit (JDK) 8 or higher
- A terminal or IDE (e.g., IntelliJ IDEA, Eclipse)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/IshworSubedi13/JavaTraineeAssestment.git
cd JavaTraineeAssestment
```
### 2. Compile the Java Files
```bash
javac ParallelCsvAggregator.java TaskScheduler.java
```
### 3. Running the Programs
Run ParallelCsvAggregator (assumes input file path as an argument):
```bash
java ParallelCsvAggregator sample_5k.csv
```
Run TaskScheduler:
```bash
java TaskScheduler
```
### Author
Ishwor Subedi


