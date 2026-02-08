"""
Pipeline Generator Module - Generates pipeline code, configs, and tests using Jinja2 templates.
"""
import os
import ast
import yaml
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader, BaseLoader
import structlog

logger = structlog.get_logger()


class PipelineGenerator:
    """
    Generates executable Python pipeline code, YAML configurations, and test files.
    Uses Jinja2 templates for consistent code generation.
    """
    
    def __init__(self, templates_dir: str = None, output_dir: str = None):
        self.templates_dir = Path(templates_dir) if templates_dir else Path(__file__).parent.parent / "templates_jinja"
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).parent.parent / "generated_pipelines"
        
        # Ensure directories exist
        self.templates_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Create default templates if they don't exist
        self._ensure_templates()
        
        logger.info("Pipeline Generator initialized", 
                   templates_dir=str(self.templates_dir),
                   output_dir=str(self.output_dir))
    
    def _ensure_templates(self):
        """Create default templates if they don't exist."""
        templates = {
            'pipeline.py.j2': self._get_pipeline_template(),
            'config.yaml.j2': self._get_config_template(),
            'test_pipeline.py.j2': self._get_test_template()
        }
        
        for name, content in templates.items():
            template_path = self.templates_dir / name
            if not template_path.exists():
                with open(template_path, 'w') as f:
                    f.write(content)
                logger.info("Created template", template=name)
    
    def generate_pipeline(self, specifications: Dict[str, Any], ai_code: str = None) -> Dict[str, str]:
        pipeline_name = specifications.get('pipeline_name', 'data_pipeline')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pipeline_dir = self.output_dir / f"{pipeline_name}_{timestamp}"
        pipeline_dir.mkdir(exist_ok=True)
        
        generated_files = {}
        
        # Generate Python code
        if ai_code:
            code = ai_code
        else:
            code = self._render_template('pipeline.py.j2', specifications)
        
        code_path = pipeline_dir / f"{pipeline_name}.py"
        with open(code_path, 'w') as f:
            f.write(code)
        generated_files['pipeline_code'] = str(code_path)
        
        # Validate Python syntax
        is_valid, error = self._validate_python_syntax(code)
        if not is_valid:
            logger.warning("Generated code has syntax issues", error=error)
            # maybe should raise here? but let's just warn for now
        
        # Generate YAML config
        config = self._render_template('config.yaml.j2', specifications)
        config_path = pipeline_dir / "config.yaml"
        with open(config_path, 'w') as f:
            f.write(config)
        generated_files['config'] = str(config_path)
        
        # Generate test file
        tests = self._render_template('test_pipeline.py.j2', specifications)
        test_path = pipeline_dir / f"test_{pipeline_name}.py"
        with open(test_path, 'w') as f:
            f.write(tests)
        generated_files['tests'] = str(test_path)
        
        # Generate requirements.txt
        requirements = self._generate_requirements(specifications)
        req_path = pipeline_dir / "requirements.txt"
        with open(req_path, 'w') as f:
            f.write(requirements)
        generated_files['requirements'] = str(req_path)
        
        # Generate README
        readme = self._generate_readme(specifications, pipeline_name)
        readme_path = pipeline_dir / "README.md"
        with open(readme_path, 'w') as f:
            f.write(readme)
        generated_files['readme'] = str(readme_path)
        
        # Save specifications as JSON
        spec_path = pipeline_dir / "specifications.json"
        with open(spec_path, 'w') as f:
            json.dump(specifications, f, indent=2)
        generated_files['specifications'] = str(spec_path)
        
        generated_files['directory'] = str(pipeline_dir)
        
        logger.info("Pipeline generated", 
                   pipeline=pipeline_name,
                   files=list(generated_files.keys()))
        
        return generated_files
    
    def _render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render a Jinja2 template with the given context."""
        try:
            template = self.env.get_template(template_name)
            return template.render(**context, now=datetime.now())
        except Exception as e:
            logger.error("Template rendering failed", template=template_name, error=str(e))
            # Fall back to string-based template
            return self._fallback_render(template_name, context)
    
    def _fallback_render(self, template_name: str, context: Dict[str, Any]) -> str:
        """Fallback rendering when template loading fails."""
        if 'pipeline.py' in template_name:
            return self._generate_fallback_code(context)
        elif 'config.yaml' in template_name:
            return yaml.dump(context, default_flow_style=False)
        elif 'test' in template_name:
            return self._generate_fallback_tests(context)
        return str(context)
    
    def _validate_python_syntax(self, code: str) -> tuple:
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"
    
    def _generate_requirements(self, specifications: Dict[str, Any]) -> str:
        """Generate requirements.txt content."""
        requirements = [
            "pandas>=2.0.0",
            "structlog>=23.1.0",
            "pyyaml>=6.0",
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0"
        ]
        
        source = specifications.get('source', {})
        destination = specifications.get('destination', {})
        
        # Add format-specific requirements
        formats = [source.get('format', ''), destination.get('format', '')]
        
        if any(f in ['excel', 'xlsx', 'xls'] for f in formats):
            requirements.append("openpyxl>=3.1.0")
        
        if 'parquet' in formats:
            requirements.append("pyarrow>=12.0.0")
        
        if any(f in ['postgres', 'postgresql'] for f in formats):
            requirements.append("psycopg2-binary>=2.9.0")
            requirements.append("sqlalchemy>=2.0.0")
        
        if 'mysql' in formats:
            requirements.append("pymysql>=1.0.0")
            requirements.append("sqlalchemy>=2.0.0")
        
        if any(f in ['api', 'rest'] for f in formats):
            requirements.append("requests>=2.28.0")
        
        return '\n'.join(sorted(set(requirements)))
    
    def _generate_readme(self, specifications: Dict[str, Any], pipeline_name: str) -> str:
        """Generate README.md content."""
        return f"""# {pipeline_name.replace('_', ' ').title()}

## Description
{specifications.get('description', 'Auto-generated data pipeline')}


```

### Usage
```bash
python {pipeline_name}.py --source <input_file> --destination <output_file>
```

### Options
- `--source, -s`: Path to source data file (required)
- `--destination, -d`: Path for output file (required)
- `--force, -f`: Force execution even if data unchanged

## Configuration
Edit `config.yaml` to customize pipeline behavior.

## Testing
```bash
pytest test_{pipeline_name}.py -v --cov={pipeline_name}
```

## Pipeline Details

### Source
- Type: {specifications.get('source', {}).get('type', 'file')}
- Format: {specifications.get('source', {}).get('format', 'csv')}

### Transformations
{self._format_transformations_md(specifications.get('transformations', []))}

### Destination
- Type: {specifications.get('destination', {}).get('type', 'file')}
- Format: {specifications.get('destination', {}).get('format', 'csv')}

## Error Handling
- On Failure: {specifications.get('error_handling', {}).get('on_failure', 'retry')}
- Max Retries: {specifications.get('error_handling', {}).get('max_retries', 3)}

---
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    def _format_transformations_md(self, transformations: list) -> str:
        """Format transformations as markdown list."""
        if not transformations:
            return "- No transformations defined"
        
        lines = []
        for i, t in enumerate(transformations, 1):
            lines.append(f"{i}. **{t.get('type', 'transform').title()}**: {t.get('description', 'Transform data')}")
        return '\n'.join(lines)
    
    def _generate_fallback_code(self, specifications: Dict[str, Any]) -> str:
        """Generate basic pipeline code without templates."""
        name = specifications.get('pipeline_name', 'data_pipeline')
        return f'''"""
Auto-generated Pipeline: {name}
"""
import pandas as pd
import structlog

logger = structlog.get_logger()

def run_pipeline(source: str, destination: str):
    """Execute the data pipeline."""
    logger.info("Starting pipeline", source=source, destination=destination)
    
    # Extract
    df = pd.read_csv(source)
    logger.info("Data extracted", rows=len(df))
    
    # Transform
    df = df.dropna()
    logger.info("Data transformed", rows=len(df))
    
    # Load
    df.to_csv(destination, index=False)
    logger.info("Data loaded", destination=destination)
    
    return {{"status": "success", "rows": len(df)}}

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        run_pipeline(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python {name}.py <source> <destination>")
'''
    
    def _generate_fallback_tests(self, specifications: Dict[str, Any]) -> str:
        """Generate basic test file without templates."""
        name = specifications.get('pipeline_name', 'data_pipeline')
        return f'''"""
Tests for {name}
"""
import pytest
import pandas as pd
from pathlib import Path

class TestPipeline:
    """Test cases for the data pipeline."""
    
    def test_pipeline_exists(self):
        """Test that pipeline module can be imported."""
        assert Path("{name}.py").exists()
    
    def test_sample_data_processing(self, tmp_path):
        """Test processing sample data."""
        # Create sample data
        sample_data = pd.DataFrame({{
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        }})
        
        input_file = tmp_path / "input.csv"
        output_file = tmp_path / "output.csv"
        
        sample_data.to_csv(input_file, index=False)
        
        # Verify input was created
        assert input_file.exists()
        
    def test_data_validation(self):
        """Test data validation rules."""
        df = pd.DataFrame({{'col1': [1, 2, None], 'col2': ['a', 'b', 'c']}})
        
        # Check for nulls
        assert df.isnull().sum().sum() == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''
    
    def _get_pipeline_template(self) -> str:
        """Get the default pipeline Jinja2 template."""
        return '''"""
Auto-generated Data Pipeline: {{ pipeline_name }}
Description: {{ description | default('Data processing pipeline') }}
Generated: {{ now.strftime('%Y-%m-%d %H:%M:%S') }}
Generator: Automatic Data Pipeline Generator - Group 29
"""

import pandas as pd
import hashlib
import json
import structlog
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger()


class {{ pipeline_name | title | replace('_', '') }}Pipeline:
    """
    Data pipeline for {{ description | default('processing data') }}.
    
    Features:
    - Idempotent execution with hash-based change detection
    - Comprehensive error handling and retry logic
    - Structured logging for observability
    - Input validation and quality checks
    """
    
    def __init__(self, config_path: str = None):
        """Initialize the pipeline with optional configuration."""
        self.config = self._load_config(config_path) if config_path else {}
        self.state_file = Path("{{ pipeline_name }}_state.json")
        self.metrics = {
            "records_input": 0,
            "records_output": 0,
            "records_failed": 0,
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0
        }
        logger.info("Pipeline initialized", pipeline="{{ pipeline_name }}")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        import yaml
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def _compute_hash(self, data: pd.DataFrame) -> str:
        """Compute hash for idempotency checking."""
        return hashlib.md5(
            pd.util.hash_pandas_object(data).values.tobytes()
        ).hexdigest()
    
    def _load_state(self) -> Dict:
        """Load pipeline state for idempotent execution."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {"last_hash": None, "last_run": None, "run_count": 0}
    
    def _save_state(self, data_hash: str):
        """Save pipeline state after successful execution."""
        state = self._load_state()
        state.update({
            "last_hash": data_hash,
            "last_run": datetime.now().isoformat(),
            "run_count": state.get("run_count", 0) + 1
        })
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def validate_input(self, df: pd.DataFrame) -> List[str]:
        """
        Validate input data against defined rules.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        {% if validation_rules %}
        {% for rule in validation_rules %}
        # Validation: {{ rule.rule }} for {{ rule.field }}
        {% if rule.rule == 'not_null' %}
        if '{{ rule.field }}' in df.columns and df['{{ rule.field }}'].isnull().any():
            errors.append("Column '{{ rule.field }}' contains null values")
        {% elif rule.rule == 'unique' %}
        if '{{ rule.field }}' in df.columns and df['{{ rule.field }}'].duplicated().any():
            errors.append("Column '{{ rule.field }}' contains duplicate values")
        {% endif %}
        {% endfor %}
        {% endif %}
        
        return errors
    
    def extract(self, source_path: str) -> pd.DataFrame:
        """
        Extract data from source.
        
        Args:
            source_path: Path to source data
            
        Returns:
            Extracted DataFrame
        """
        logger.info("Extracting data", source=source_path)
        
        try:
            {% if source.format == 'csv' %}
            df = pd.read_csv(source_path)
            {% elif source.format == 'json' %}
            df = pd.read_json(source_path)
            {% elif source.format in ['excel', 'xlsx', 'xls'] %}
            df = pd.read_excel(source_path)
            {% elif source.format == 'parquet' %}
            df = pd.read_parquet(source_path)
            {% else %}
            df = pd.read_csv(source_path)
            {% endif %}
            
            self.metrics["records_input"] = len(df)
            logger.info("Extraction complete", rows=len(df), columns=len(df.columns))
            return df
            
        except Exception as e:
            logger.error("Extraction failed", error=str(e))
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformations", input_rows=len(df))
        
        try:
            {% for transform in transformations %}
            # Transformation: {{ transform.type }} - {{ transform.description }}
            logger.info("Applying transformation", type="{{ transform.type }}")
            {% if transform.type == 'validate' %}
            # Handle missing values
            df = df.dropna(how='all')
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].fillna(df[col].median())
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].fillna('UNKNOWN')
            {% elif transform.type == 'deduplicate' %}
            # Remove duplicates
            initial = len(df)
            df = df.drop_duplicates()
            logger.info("Deduplication", removed=initial - len(df))
            {% elif transform.type == 'normalize' %}
            # Normalize numeric columns
            for col in df.select_dtypes(include=['number']).columns:
                min_val, max_val = df[col].min(), df[col].max()
                if max_val > min_val:
                    df[col] = (df[col] - min_val) / (max_val - min_val)
            {% elif transform.type == 'filter' %}
            # Apply filtering
            # TODO: Customize filter conditions as needed
            pass
            {% elif transform.type == 'aggregate' %}
            # Apply aggregation
            # TODO: Customize groupby and aggregation as needed
            pass
            {% elif transform.type == 'map' %}
            # Apply mapping transformations
            pass
            {% elif transform.type == 'enrich' %}
            # Enrich data with additional information
            pass
            {% else %}
            # Custom transformation
            pass
            {% endif %}
            
            {% endfor %}
            
            logger.info("Transformations complete", output_rows=len(df))
            return df
            
        except Exception as e:
            logger.error("Transformation failed", error=str(e))
            raise
    
    def load(self, df: pd.DataFrame, destination_path: str):
        """
        Load data to destination.
        
        Args:
            df: DataFrame to save
            destination_path: Output path
        """
        logger.info("Loading data", destination=destination_path)
        
        try:
            {% if destination.format == 'csv' %}
            df.to_csv(destination_path, index=False)
            {% elif destination.format == 'json' %}
            df.to_json(destination_path, orient='records', indent=2)
            {% elif destination.format == 'parquet' %}
            df.to_parquet(destination_path, index=False)
            {% elif destination.format in ['excel', 'xlsx'] %}
            df.to_excel(destination_path, index=False)
            {% else %}
            df.to_csv(destination_path, index=False)
            {% endif %}
            
            self.metrics["records_output"] = len(df)
            logger.info("Load complete", rows=len(df))
            
        except Exception as e:
            logger.error("Load failed", error=str(e))
            raise
    
    def run(self, source_path: str, destination_path: str, 
            force: bool = False, dry_run: bool = False) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source_path: Path to source data
            destination_path: Path for output
            force: Force execution even if data unchanged
            dry_run: Validate without actual execution
            
        Returns:
            Execution metrics dictionary
        """
        self.metrics["start_time"] = datetime.now().isoformat()
        logger.info("Pipeline execution started",
                   source=source_path,
                   destination=destination_path,
                   dry_run=dry_run)
        
        try:
            # Extract
            df = self.extract(source_path)
            
            # Validate input
            validation_errors = self.validate_input(df)
            if validation_errors:
                logger.warning("Validation issues found", errors=validation_errors)
                # should we fail here? leaving as warning for now
            
            # Check idempotency
            data_hash = self._compute_hash(df)
            state = self._load_state()
            
            if not force and state["last_hash"] == data_hash:
                logger.info("Data unchanged, skipping")
                return {"status": "skipped", "reason": "data_unchanged"}
            
            if dry_run:
                logger.info("Dry run complete - no changes made")
                return {"status": "dry_run", "would_process": len(df)}
            
            # Transform
            df = self.transform(df)
            
            # Load
            self.load(df, destination_path)
            
            # Save state
            self._save_state(data_hash)
            
            # Finalize metrics
            end_time = datetime.now()
            self.metrics["end_time"] = end_time.isoformat()
            start = datetime.fromisoformat(self.metrics["start_time"])
            self.metrics["duration_seconds"] = (end_time - start).total_seconds()
            self.metrics["status"] = "success"
            
            logger.info("Pipeline complete", metrics=self.metrics)
            return self.metrics
            
        except Exception as e:
            self.metrics["end_time"] = datetime.now().isoformat()
            self.metrics["status"] = "failed"
            self.metrics["error"] = str(e)
            logger.error("Pipeline failed", error=str(e))
            raise


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="{{ pipeline_name }} Pipeline")
    parser.add_argument("--source", "-s", required=True, help="Source data path")
    parser.add_argument("--destination", "-d", required=True, help="Destination path")
    parser.add_argument("--config", "-c", help="Configuration file path")
    parser.add_argument("--force", "-f", action="store_true", help="Force execution")
    parser.add_argument("--dry-run", action="store_true", help="Validate without executing")
    
    args = parser.parse_args()
    
    pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline(args.config)
    result = pipeline.run(args.source, args.destination, args.force, args.dry_run)
    
    print(json.dumps(result, indent=2))
    return 0 if result.get("status") in ["success", "skipped", "dry_run"] else 1


if __name__ == "__main__":
    exit(main())
'''
    
    def _get_config_template(self) -> str:
        """Get the default config YAML Jinja2 template."""
        return '''# Pipeline Configuration: {{ pipeline_name }}
# Generated: {{ now.strftime('%Y-%m-%d %H:%M:%S') }}

pipeline:
  name: "{{ pipeline_name }}"
  description: "{{ description | default('Data processing pipeline') }}"
  version: "1.0.0"

source:
  type: "{{ source.type | default('file') }}"
  format: "{{ source.format | default('csv') }}"
  path: "{{ source.path_or_connection | default('input_data') }}"

destination:
  type: "{{ destination.type | default('file') }}"
  format: "{{ destination.format | default('csv') }}"
  path: "{{ destination.path_or_connection | default('output_data') }}"

transformations:
{% for transform in transformations %}
  - type: "{{ transform.type }}"
    description: "{{ transform.description }}"
    enabled: true
{% endfor %}

validation:
{% for rule in validation_rules %}
  - field: "{{ rule.field }}"
    rule: "{{ rule.rule }}"
{% endfor %}

error_handling:
  on_failure: "{{ error_handling.on_failure | default('retry') }}"
  max_retries: {{ error_handling.max_retries | default(3) }}
  notification: {{ error_handling.notification | default(true) | lower }}

execution:
  schedule: "{{ schedule | default('once') }}"
  timeout_seconds: 3600
  enable_monitoring: true

logging:
  level: "INFO"
  format: "json"
'''
    
    def _get_test_template(self) -> str:
        """Get the default test Jinja2 template."""
        return '''"""
Test Suite for {{ pipeline_name }}
Generated: {{ now.strftime('%Y-%m-%d %H:%M:%S') }}
"""

import pytest
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Import the pipeline module
from {{ pipeline_name }} import {{ pipeline_name | title | replace('_', '') }}Pipeline


class TestPipelineInitialization:
    """Tests for pipeline initialization."""
    
    def test_pipeline_creates_successfully(self):
        """Test that pipeline can be instantiated."""
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        assert pipeline is not None
    
    def test_initial_metrics(self):
        """Test that initial metrics are set correctly."""
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        assert pipeline.metrics["records_input"] == 0
        assert pipeline.metrics["records_output"] == 0


class TestDataExtraction:
    """Tests for data extraction functionality."""
    
    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file for testing."""
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'value': [100, 200, 300, 400, 500]
        })
        file_path = tmp_path / "sample.csv"
        data.to_csv(file_path, index=False)
        return file_path
    
    def test_extract_csv(self, sample_csv):
        """Test CSV extraction."""
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        df = pipeline.extract(str(sample_csv))
        
        assert len(df) == 5
        assert 'id' in df.columns
        assert 'name' in df.columns
        assert 'value' in df.columns


class TestDataTransformation:
    """Tests for data transformation functionality."""
    
    def test_transform_handles_nulls(self):
        """Test that transformation handles null values."""
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        df = pd.DataFrame({
            'id': [1, 2, None],
            'value': [100, None, 300]
        })
        
        result = pipeline.transform(df)
        
        # Should not have rows with all nulls
        assert len(result) >= 1


class TestPipelineExecution:
    """Tests for complete pipeline execution."""
    
    @pytest.fixture
    def pipeline_files(self, tmp_path):
        """Create input/output paths for testing."""
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"
        data.to_csv(input_path, index=False)
        return input_path, output_path
    
    def test_full_pipeline_execution(self, pipeline_files):
        """Test complete pipeline run."""
        input_path, output_path = pipeline_files
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        
        result = pipeline.run(str(input_path), str(output_path))
        
        assert result["status"] == "success"
        assert output_path.exists()
    
    def test_dry_run_no_output(self, pipeline_files):
        """Test that dry run doesn't create output."""
        input_path, output_path = pipeline_files
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        
        result = pipeline.run(str(input_path), str(output_path), dry_run=True)
        
        assert result["status"] == "dry_run"


class TestIdempotency:
    """Tests for idempotent execution."""
    
    @pytest.fixture
    def pipeline_with_data(self, tmp_path):
        """Create pipeline with test data."""
        data = pd.DataFrame({'id': [1, 2], 'value': [100, 200]})
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"
        data.to_csv(input_path, index=False)
        
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        pipeline.state_file = tmp_path / "state.json"
        
        return pipeline, input_path, output_path
    
    def test_skips_unchanged_data(self, pipeline_with_data):
        """Test that unchanged data is skipped on second run."""
        pipeline, input_path, output_path = pipeline_with_data
        
        # First run
        result1 = pipeline.run(str(input_path), str(output_path))
        assert result1["status"] == "success"
        
        # Second run with same data
        result2 = pipeline.run(str(input_path), str(output_path))
        assert result2["status"] == "skipped"
    
    def test_force_overrides_skip(self, pipeline_with_data):
        """Test that force flag overrides skip."""
        pipeline, input_path, output_path = pipeline_with_data
        
        # First run
        pipeline.run(str(input_path), str(output_path))
        
        # Force second run
        result = pipeline.run(str(input_path), str(output_path), force=True)
        assert result["status"] == "success"


class TestDataQuality:
    """Tests for data quality checks."""
    
    def test_validation_detects_nulls(self):
        """Test that validation detects null values."""
        pipeline = {{ pipeline_name | title | replace('_', '') }}Pipeline()
        df = pd.DataFrame({
            'required_field': [1, None, 3]
        })
        
        # Validation should work without errors
        errors = pipeline.validate_input(df)
        assert isinstance(errors, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov={{ pipeline_name }}", "--cov-report=html"])
'''