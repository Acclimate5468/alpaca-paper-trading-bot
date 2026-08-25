# Alpaca Paper-Trading Bot

A single-process Python automation project that reads market data from Alpaca, evaluates a configurable EMA-based signal, sizes long equity entries, and manages trailing-stop protection in an Alpaca paper account.

> This repository is a software portfolio project. It is not investment advice, a trading recommendation, or evidence of trading performance. Running the script with valid credentials can place paper orders.

## What it does

- Downloads recent bars for a configurable list of U.S. equities and ETFs.
- Calculates short/long EMAs, a volume baseline, and an optional RSI filter.
- Evaluates an EMA trend/crossover condition once per bar bucket.
- Sizes entries from an account-level risk budget and a per-symbol cash cap.
- Limits concurrent positions and new buys per bucket.
- Reconciles open orders and keeps one trailing-stop sell order per position.
- Stops new entries after a configurable daily drawdown threshold.
- Persists lightweight runtime state and writes operational logs locally.
- Handles SIGINT/SIGTERM for a graceful shutdown.

## Safety boundaries

This portfolio build refuses to start when `PAPER` is not `true`. Credentials are read only from environment variables, and local logs, state, and `.env` files are ignored by Git.

The safeguards reduce operational mistakes; they do not make the strategy profitable or production-ready.

## Setup

Requires Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Add Alpaca paper-account credentials to `.env`, then load the variables and run:

```bash
set -a
source .env
set +a
python ffb.py
```

Stop with `Ctrl+C`.

## Configuration

The example environment file documents the main controls:

- Alpaca credentials and data feed
- EMA, volume, and RSI settings
- Lookback and polling intervals
- Risk budget, exposure cap, and position limits
- Trailing-stop and daily-loss settings
- Local state path and log level

## Design notes

The bot is long-only and uses a fixed watchlist in `ffb.py`. The default data feed is IEX. It does not include a backtest, performance report, dashboard, database, or automated test suite. Review the strategy and Alpaca API behavior independently before using it beyond a paper account.

## Security

Never commit API keys, account identifiers, brokerage exports, logs, or state files. If a real credential is ever exposed, revoke it at the provider; removing it from a later commit does not erase it from Git history.
