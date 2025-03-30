# Stablecoin Analytics Dashboard

A modular Python application for analyzing and visualizing stablecoin data from DeFi Llama.

## Features

- Real-time stablecoin data fetching from DeFi Llama API
- Interactive dashboard with multiple visualizations:
  - Market cap trends
  - Chain distribution
  - Daily changes
  - Top yielding pools
  - Price volatility
- Modular architecture for easy extension
- Asynchronous data fetching for better performance
- Type-safe code with comprehensive documentation

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd stables
```

2. Install dependencies using Poetry:
```bash
poetry install
```

## Usage

1. Activate the virtual environment:
```bash
poetry shell
```

2. Run the application:
```bash
python -m stables
```

3. Open your browser and navigate to `http://localhost:8050`

## Project Structure

```
stables/
├── src/
│   └── stables/
│       ├── api/
│       │   └── defi_llama.py      # DeFi Llama API client
│       ├── data/
│       │   └── processor.py       # Data processing module
│       ├── visualization/
│       │   └── dashboard.py       # Dashboard module
│       └── __main__.py           # Main application entry point
├── tests/                        # Test files
├── data/                         # Data storage
│   ├── raw/                      # Raw data
│   └── processed/                # Processed data
├── pyproject.toml                # Project configuration
└── README.md                     # Project documentation
```

## Development

### Adding New Features

1. Create new modules in the appropriate directory
2. Add type hints and docstrings
3. Write tests for new functionality
4. Update the dashboard if adding new visualizations

### Running Tests

```bash
poetry run pytest
```

### Code Style

The project uses:
- Ruff for linting and formatting
- MyPy for type checking
- Google-style docstrings

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
