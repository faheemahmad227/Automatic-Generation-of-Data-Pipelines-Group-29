"""
Test Suite for Data Pipeline Generator
DDE Project - Group 29
"""
import pytest
import json
import tempfile
import pandas as pd
from pathlib import Path
import sys

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.source_analyzer import SourceAnalyzer
from app.ai_analyzer import AIAnalyzer
from app.pipeline_generator import PipelineGenerator


class TestSourceAnalyzer:
    """Tests for the Source Analyzer module."""
    
    @pytest.fixture
    def analyzer(self):
        """Create a SourceAnalyzer instance."""
        return SourceAnalyzer()
    
    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', None],
            'value': [100.5, 200.0, None, 400.5, 500.0],
            'category': ['A', 'B', 'A', 'B', 'A']
        })
        file_path = tmp_path / "test_data.csv"
        data.to_csv(file_path, index=False)
        return file_path
    
    @pytest.fixture
    def sample_json(self, tmp_path):
        """Create a sample JSON file for testing."""
        data = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300}
        ]
        file_path = tmp_path / "test_data.json"
        with open(file_path, 'w') as f:
            json.dump(data, f)
        return file_path
    
    def test_detect_format_csv(self, analyzer):
        """Test CSV format detection."""
        assert analyzer.detect_format("data.csv") == "csv"
        assert analyzer.detect_format("path/to/file.CSV") == "csv"
    
    def test_detect_format_json(self, analyzer):
        """Test JSON format detection."""
        assert analyzer.detect_format("data.json") == "json"
    
    def test_detect_format_excel(self, analyzer):
        """Test Excel format detection."""
        assert analyzer.detect_format("data.xlsx") == "excel"
        assert analyzer.detect_format("data.xls") == "excel"
    
    def test_detect_format_parquet(self, analyzer):
        """Test Parquet format detection."""
        assert analyzer.detect_format("data.parquet") == "parquet"
    
    def test_detect_format_unknown(self, analyzer):
        """Test unknown format detection."""
        assert analyzer.detect_format("data.xyz") == "unknown"
    
    def test_load_csv_data(self, analyzer, sample_csv):
        """Test loading CSV data."""
        df = analyzer.load_data(str(sample_csv))
        assert len(df) == 5
        assert 'id' in df.columns
        assert 'name' in df.columns
    
    def test_load_json_data(self, analyzer, sample_json):
        """Test loading JSON data."""
        df = analyzer.load_data(str(sample_json))
        assert len(df) == 3
        assert 'id' in df.columns
    
    def test_infer_schema(self, analyzer, sample_csv):
        """Test schema inference."""
        analyzer.load_data(str(sample_csv))
        schema = analyzer.infer_schema()
        
        assert schema['row_count'] == 5
        assert schema['column_count'] == 4
        assert 'id' in schema['columns']
        assert 'name' in schema['columns']
        assert 'value' in schema['columns']
    
    def test_schema_includes_null_info(self, analyzer, sample_csv):
        """Test that schema includes null value information."""
        analyzer.load_data(str(sample_csv))
        schema = analyzer.infer_schema()
        
        # 'name' column has one null
        assert schema['columns']['name']['null_count'] == 1
        # 'value' column has one null
        assert schema['columns']['value']['null_count'] == 1
    
    def test_calculate_quality_metrics(self, analyzer, sample_csv):
        """Test quality metrics calculation."""
        analyzer.load_data(str(sample_csv))
        quality = analyzer.calculate_quality_metrics()
        
        assert 'completeness' in quality
        assert 'uniqueness' in quality
        assert 'overall_quality_score' in quality
        assert 0 <= quality['overall_quality_score'] <= 100
    
    def test_quality_detects_nulls(self, analyzer, sample_csv):
        """Test that quality metrics detect null values."""
        analyzer.load_data(str(sample_csv))
        quality = analyzer.calculate_quality_metrics()
        
        assert quality['completeness']['null_cells'] > 0
    
    def test_complete_analysis(self, analyzer, sample_csv):
        """Test complete analysis workflow."""
        schema, quality = analyzer.analyze(str(sample_csv))
        
        assert schema is not None
        assert quality is not None
        assert 'file_info' in schema
        assert schema['file_info']['file_format'] == 'csv'
    
    def test_get_preview(self, analyzer, sample_csv):
        """Test data preview functionality."""
        analyzer.load_data(str(sample_csv))
        preview = analyzer.get_preview(rows=3)
        
        assert len(preview['data']) == 3
        assert preview['total_rows'] == 5
        assert preview['preview_rows'] == 3
    
    def test_suggest_transformations(self, analyzer, sample_csv):
        """Test transformation suggestions."""
        schema, quality = analyzer.analyze(str(sample_csv))
        suggestions = analyzer.suggest_transformations(schema, quality)
        
        assert isinstance(suggestions, list)
        # Should suggest handling nulls since we have null values
        assert any(s['type'] == 'validate' for s in suggestions)


class TestAIAnalyzer:
    """Tests for the AI Analyzer module."""
    
    @pytest.fixture
    def analyzer(self):
        """Create an AIAnalyzer instance (without API key for testing)."""
        return AIAnalyzer(api_key=None)
    
    def test_initialization_without_api_key(self, analyzer):
        """Test that analyzer initializes without API key."""
        assert analyzer.client is None
    
    def test_fallback_analysis(self, analyzer):
        """Test fallback analysis when AI is unavailable."""
        description = "Create a pipeline that reads CSV data and filters invalid records"
        result = analyzer.analyze_requirements(description)
        
        assert 'pipeline_name' in result
        assert 'source' in result
        assert 'transformations' in result
        assert 'destination' in result
    
    def test_fallback_detects_csv(self, analyzer):
        """Test that fallback correctly detects CSV mention."""
        description = "Process CSV file with customer data"
        result = analyzer.analyze_requirements(description)
        
        assert result['source']['format'] == 'csv'
    
    def test_fallback_detects_json(self, analyzer):
        """Test that fallback correctly detects JSON mention."""
        description = "Read JSON data and transform it"
        result = analyzer.analyze_requirements(description)
        
        assert result['source']['format'] == 'json'
    
    def test_fallback_detects_database(self, analyzer):
        """Test that fallback correctly detects database mention."""
        description = "Load data from PostgreSQL database"
        result = analyzer.analyze_requirements(description)
        
        assert result['source']['type'] == 'database'
        assert result['source']['format'] == 'postgres'
    
    def test_fallback_detects_transformations(self, analyzer):
        """Test that fallback detects transformation keywords."""
        description = "Filter records, remove duplicates, and normalize values"
        result = analyzer.analyze_requirements(description)
        
        transformation_types = [t['type'] for t in result['transformations']]
        assert 'filter' in transformation_types
        assert 'deduplicate' in transformation_types
        assert 'normalize' in transformation_types
    
    def test_generate_template_code(self, analyzer):
        """Test template code generation."""
        specs = {
            'pipeline_name': 'test_pipeline',
            'description': 'Test pipeline',
            'source': {'type': 'file', 'format': 'csv'},
            'transformations': [
                {'type': 'validate', 'description': 'Validate data'},
                {'type': 'deduplicate', 'description': 'Remove duplicates'}
            ],
            'destination': {'type': 'file', 'format': 'csv'}
        }
        
        code = analyzer.generate_pipeline_code(specs)
        
        assert 'import pandas' in code
        assert 'class' in code
        assert 'def run' in code
        assert 'structlog' in code
    
    def test_fallback_recommendations(self, analyzer):
        """Test fallback recommendations."""
        schema = {'columns': {'col1': 'object'}}
        quality = {'null_percentage': 10, 'duplicate_percentage': 5}
        
        recommendations = analyzer.get_recommendations(schema, quality)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0


class TestPipelineGenerator:
    """Tests for the Pipeline Generator module."""
    
    @pytest.fixture
    def generator(self, tmp_path):
        """Create a PipelineGenerator instance."""
        templates_dir = tmp_path / "templates"
        output_dir = tmp_path / "output"
        templates_dir.mkdir()
        output_dir.mkdir()
        return PipelineGenerator(
            templates_dir=str(templates_dir),
            output_dir=str(output_dir)
        )
    
    @pytest.fixture
    def sample_specs(self):
        """Create sample pipeline specifications."""
        return {
            'pipeline_name': 'test_pipeline',
            'description': 'A test data pipeline',
            'source': {
                'type': 'file',
                'format': 'csv',
                'path_or_connection': 'input.csv'
            },
            'transformations': [
                {'type': 'validate', 'description': 'Validate data', 'config': {}},
                {'type': 'deduplicate', 'description': 'Remove duplicates', 'config': {}}
            ],
            'destination': {
                'type': 'file',
                'format': 'csv',
                'path_or_connection': 'output.csv'
            },
            'validation_rules': [],
            'schedule': 'once',
            'error_handling': {
                'on_failure': 'retry',
                'max_retries': 3,
                'notification': True
            }
        }
    
    def test_initialization(self, generator):
        """Test generator initialization."""
        assert generator.templates_dir.exists()
        assert generator.output_dir.exists()
    
    def test_templates_created(self, generator):
        """Test that default templates are created."""
        assert (generator.templates_dir / 'pipeline.py.j2').exists()
        assert (generator.templates_dir / 'config.yaml.j2').exists()
        assert (generator.templates_dir / 'test_pipeline.py.j2').exists()
    
    def test_generate_pipeline(self, generator, sample_specs):
        """Test complete pipeline generation."""
        result = generator.generate_pipeline(sample_specs)
        
        assert 'pipeline_code' in result
        assert 'config' in result
        assert 'tests' in result
        assert 'requirements' in result
        assert 'readme' in result
        assert 'directory' in result
    
    def test_generated_files_exist(self, generator, sample_specs):
        """Test that generated files actually exist."""
        result = generator.generate_pipeline(sample_specs)
        
        assert Path(result['pipeline_code']).exists()
        assert Path(result['config']).exists()
        assert Path(result['tests']).exists()
        assert Path(result['requirements']).exists()
    
    def test_generated_code_is_valid_python(self, generator, sample_specs):
        """Test that generated Python code is syntactically valid."""
        result = generator.generate_pipeline(sample_specs)
        
        with open(result['pipeline_code']) as f:
            code = f.read()
        
        # Should not raise SyntaxError
        import ast
        ast.parse(code)
    
    def test_generated_config_is_valid_yaml(self, generator, sample_specs):
        """Test that generated config is valid YAML."""
        import yaml
        
        result = generator.generate_pipeline(sample_specs)
        
        with open(result['config']) as f:
            config = yaml.safe_load(f)
        
        assert config is not None
        assert 'pipeline' in config
    
    def test_requirements_includes_pandas(self, generator, sample_specs):
        """Test that requirements include pandas."""
        result = generator.generate_pipeline(sample_specs)
        
        with open(result['requirements']) as f:
            requirements = f.read()
        
        assert 'pandas' in requirements
    
    def test_readme_generated(self, generator, sample_specs):
        """Test that README is generated with correct content."""
        result = generator.generate_pipeline(sample_specs)
        
        with open(result['readme']) as f:
            readme = f.read()
        
        assert 'test_pipeline' in readme.lower() or 'Test Pipeline' in readme


class TestIntegration:
    """Integration tests for the complete workflow."""
    
    @pytest.fixture
    def sample_data(self, tmp_path):
        """Create sample data for integration testing."""
        data = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Item_{i}' for i in range(1, 101)],
            'value': [i * 10.5 for i in range(1, 101)],
            'category': ['A' if i % 2 == 0 else 'B' for i in range(1, 101)]
        })
        file_path = tmp_path / "integration_test.csv"
        data.to_csv(file_path, index=False)
        return file_path
    
    def test_analyze_and_generate_workflow(self, sample_data, tmp_path):
        """Test the complete analyze -> generate workflow."""
        # Step 1: Analyze the data
        source_analyzer = SourceAnalyzer()
        schema, quality = source_analyzer.analyze(str(sample_data))
        
        assert schema['row_count'] == 100
        assert quality['overall_quality_score'] == 100  # No nulls, no duplicates
        
        # Step 2: Get AI recommendations
        ai_analyzer = AIAnalyzer(api_key=None)
        recommendations = ai_analyzer.get_recommendations(schema, quality)
        
        assert isinstance(recommendations, list)
        
        # Step 3: Generate pipeline based on analysis
        description = f"Process {schema['file_info']['file_name']} with {len(schema['columns'])} columns"
        specs = ai_analyzer.analyze_requirements(description)
        specs['pipeline_name'] = 'integration_test_pipeline'
        
        # Step 4: Generate the pipeline
        generator = PipelineGenerator(
            templates_dir=str(tmp_path / "templates"),
            output_dir=str(tmp_path / "output")
        )
        result = generator.generate_pipeline(specs)
        
        # Verify all files were generated
        assert Path(result['pipeline_code']).exists()
        assert Path(result['config']).exists()
        assert Path(result['tests']).exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=app", "--cov-report=html"])
