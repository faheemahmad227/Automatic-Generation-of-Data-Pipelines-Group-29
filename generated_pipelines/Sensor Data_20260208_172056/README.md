# Sensor Data

## Description
Process the uploaded data with the following transformations: deduplicate, normalize, filter. Based on analysis, handle 1.25% null values and 1 duplicates.


```

### Usage
```bash
python Sensor Data.py --source <input_file> --destination <output_file>
```

### Options
- `--source, -s`: Path to source data file (required)
- `--destination, -d`: Path for output file (required)
- `--force, -f`: Force execution even if data unchanged

## Configuration
Edit `config.yaml` to customize pipeline behavior.

## Testing
```bash
pytest test_Sensor Data.py -v --cov=Sensor Data
```

## Pipeline Details

### Source
- Type: file
- Format: csv

### Transformations
1. **Validate**: Handle missing values
2. **Filter**: Filter data based on conditions
3. **Normalize**: Normalize data values
4. **Deduplicate**: Remove duplicates

### Destination
- Type: file
- Format: csv

## Error Handling
- On Failure: retry
- Max Retries: 3

---
Generated on: 2026-02-08 17:20:56
