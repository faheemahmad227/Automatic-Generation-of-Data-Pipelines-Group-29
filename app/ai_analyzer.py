"""
AI Analyzer Module - Handles LLM integration for pipeline generation.
Uses OpenAI-compatible API (supports Uni Paderborn AI Gateway).
"""
import os
import json
import re
from typing import Dict, Any, Optional, List
from openai import OpenAI
import structlog

logger = structlog.get_logger()

# ============================================================================
# 🔑 API CONFIGURATION - UPDATE THESE VALUES WITH YOUR CREDENTIALS
# ============================================================================

# Uni Paderborn AI Gateway Configuration
API_KEY = "YOUR_API_KEY_HERE"  # <-- PUT YOUR API KEY HERE
BASE_URL = "https://ai-gateway.2.2021.2.2.2.2.2.2/v1"  # <-- UPDATE IF NEEDED

# Model name (gpt-4o-mini works well for this application)
MODEL_NAME = "gpt-4o-mini"

# ============================================================================
# After updating API_KEY above, save the file and run: python main.py
# ============================================================================


class AIAnalyzer:
    """
    AI-powered analyzer for understanding user requirements and generating pipeline specifications.
    """
    
    def __init__(self, api_key: str = None, base_url: str = None):
        """Initialize the AI Analyzer with API credentials."""
        # Use provided values, or fall back to configured constants, or environment variables
        self.api_key = api_key or API_KEY if API_KEY != "YOUR_API_KEY_HERE" else os.getenv("OPENAI_API_KEY") or os.getenv("AI_GATEWAY_API_KEY")
        self.base_url = base_url or BASE_URL or os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.model_name = MODEL_NAME
        
        if not self.api_key:
            logger.warning("No API key provided. AI features will be limited.")
            self.client = None
        else:
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
            logger.info("AI Analyzer initialized", base_url=self.base_url)
    
    def analyze_requirements(self, user_description: str, schema_info: Dict = None) -> Dict[str, Any]:
        """
        Analyze user's natural language description and extract pipeline requirements.
        
        Args:
            user_description: Natural language description of the pipeline
            schema_info: Optional schema information from source analysis
            
        Returns:
            Dictionary containing extracted pipeline specifications
        """
        if not self.client:
            return self._fallback_analysis(user_description)
        
        schema_context = ""
        if schema_info:
            schema_context = f"\n\nAvailable data schema:\n{json.dumps(schema_info, indent=2)}"
        
        system_prompt = """You are a data pipeline architect. Analyze the user's requirements and extract structured pipeline specifications.

Return a JSON object with:
{
    "pipeline_name": "descriptive_name",
    "description": "brief description",
    "source": {
        "type": "file|database|api",
        "format": "csv|json|excel|parquet|postgres|mysql|rest",
        "path_or_connection": "path or connection string pattern"
    },
    "transformations": [
        {
            "type": "normalize|aggregate|filter|map|validate|deduplicate|enrich|custom",
            "description": "what this step does",
            "config": {}
        }
    ],
    "destination": {
        "type": "file|database|api",
        "format": "csv|json|parquet|postgres|mysql",
        "path_or_connection": "output path pattern"
    },
    "validation_rules": [
        {"field": "field_name", "rule": "not_null|unique|range|regex", "params": {}}
    ],
    "schedule": "once|hourly|daily|weekly|custom",
    "error_handling": {
        "on_failure": "stop|skip|retry",
        "max_retries": 3,
        "notification": true
    }
}

Be specific and practical. Use the schema information if provided."""

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"{user_description}{schema_context}"}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content
            # Extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                result = json.loads(json_match.group())
                logger.info("Requirements analyzed successfully", pipeline_name=result.get('pipeline_name'))
                return result
            else:
                logger.warning("Could not parse AI response, using fallback")
                return self._fallback_analysis(user_description)
                
        except Exception as e:
            logger.error("AI analysis failed", error=str(e))
            return self._fallback_analysis(user_description)
    
    def generate_pipeline_code(self, specifications: Dict[str, Any]) -> str:
        """
        Generate Python pipeline code from specifications.
        
        Args:
            specifications: Pipeline specifications dictionary
            
        Returns:
            Generated Python code as string
        """
        if not self.client:
            return self._generate_template_code(specifications)
        
        system_prompt = """You are a Python data engineering expert. Generate production-ready pipeline code.

Requirements:
1. Use pandas for data manipulation
2. Include comprehensive error handling with try/except blocks
3. Add structured logging using structlog
4. Implement input validation
5. Support idempotent execution (hash-based change detection)
6. Include docstrings and type hints
7. Follow PEP 8 style guidelines

Generate ONLY the Python code, no explanations. The code should be immediately executable."""

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate a complete Python pipeline based on:\n{json.dumps(specifications, indent=2)}"}
                ],
                temperature=0.2,
                max_tokens=4000
            )
            
            code = response.choices[0].message.content
            # Clean up code blocks if present
            code = re.sub(r'^```python\n?', '', code)
            code = re.sub(r'\n?```$', '', code)
            
            logger.info("Pipeline code generated", lines=len(code.split('\n')))
            return code
            
        except Exception as e:
            logger.error("Code generation failed", error=str(e))
            return self._generate_template_code(specifications)
    
    def get_recommendations(self, schema_info: Dict, quality_metrics: Dict) -> List[Dict]:
        """
        Get AI-powered recommendations based on data analysis.
        
        Args:
            schema_info: Schema information from source analyzer
            quality_metrics: Data quality metrics
            
        Returns:
            List of recommendation dictionaries
        """
        if not self.client:
            return self._fallback_recommendations(schema_info, quality_metrics)
        
        prompt = f"""Based on this data schema and quality metrics, provide practical recommendations:

Schema: {json.dumps(schema_info, indent=2)}
Quality Metrics: {json.dumps(quality_metrics, indent=2)}

Return a JSON array of recommendations:
[
    {{"type": "transformation|validation|optimization", "priority": "high|medium|low", "title": "short title", "description": "detailed recommendation"}}
]"""

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
                max_tokens=1500
            )
            
            content = response.choices[0].message.content
            json_match = re.search(r'\[[\s\S]*\]', content)
            if json_match:
                return json.loads(json_match.group())
            return self._fallback_recommendations(schema_info, quality_metrics)
            
        except Exception as e:
            logger.error("Recommendations generation failed", error=str(e))
            return self._fallback_recommendations(schema_info, quality_metrics)
    
    def _fallback_analysis(self, user_description: str) -> Dict[str, Any]:
        """Provide basic analysis when AI is unavailable."""
        # Extract keywords for basic analysis
        description_lower = user_description.lower()
        
        # Detect source type
        source_type = "file"
        source_format = "csv"
        if "database" in description_lower or "sql" in description_lower:
            source_type = "database"
            source_format = "postgres" if "postgres" in description_lower else "mysql"
        elif "api" in description_lower or "rest" in description_lower:
            source_type = "api"
            source_format = "rest"
        elif "json" in description_lower:
            source_format = "json"
        elif "excel" in description_lower or "xlsx" in description_lower:
            source_format = "excel"
        
        # Detect transformations
        transformations = []
        if any(word in description_lower for word in ["clean", "missing", "null"]):
            transformations.append({"type": "validate", "description": "Handle missing values", "config": {}})
        if any(word in description_lower for word in ["filter", "where", "condition"]):
            transformations.append({"type": "filter", "description": "Filter data based on conditions", "config": {}})
        if any(word in description_lower for word in ["aggregate", "sum", "count", "average", "group"]):
            transformations.append({"type": "aggregate", "description": "Aggregate data", "config": {}})
        if any(word in description_lower for word in ["normalize", "standardize", "scale"]):
            transformations.append({"type": "normalize", "description": "Normalize data values", "config": {}})
        if any(word in description_lower for word in ["duplicate", "unique", "distinct"]):
            transformations.append({"type": "deduplicate", "description": "Remove duplicates", "config": {}})
        
        if not transformations:
            transformations.append({"type": "map", "description": "Transform data fields", "config": {}})
        
        return {
            "pipeline_name": "generated_pipeline",
            "description": user_description[:200],
            "source": {
                "type": source_type,
                "format": source_format,
                "path_or_connection": "input_data"
            },
            "transformations": transformations,
            "destination": {
                "type": "file",
                "format": "csv",
                "path_or_connection": "output_data.csv"
            },
            "validation_rules": [],
            "schedule": "once",
            "error_handling": {
                "on_failure": "retry",
                "max_retries": 3,
                "notification": True
            }
        }
    
    def _generate_template_code(self, specifications: Dict[str, Any]) -> str:
        """Generate basic template code when AI is unavailable."""
        pipeline_name = specifications.get('pipeline_name', 'data_pipeline')
        source = specifications.get('source', {})
        transformations = specifications.get('transformations', [])
        destination = specifications.get('destination', {})
        
        code = f'''"""
Auto-generated Data Pipeline: {pipeline_name}
Description: {specifications.get('description', 'Data processing pipeline')}
Generated by: Automatic Data Pipeline Generator
"""

import pandas as pd
import hashlib
import json
import structlog
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

# Configure logging
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


class {pipeline_name.title().replace('_', '')}Pipeline:
    """
    Data pipeline for {specifications.get('description', 'processing data')}.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the pipeline with optional configuration."""
        self.config = config or {{}}
        self.state_file = Path("pipeline_state.json")
        self.metrics = {{
            "records_processed": 0,
            "records_failed": 0,
            "start_time": None,
            "end_time": None
        }}
        logger.info("Pipeline initialized", pipeline="{pipeline_name}")
    
    def _compute_hash(self, data: pd.DataFrame) -> str:
        """Compute hash for idempotency checking."""
        return hashlib.md5(pd.util.hash_pandas_object(data).values.tobytes()).hexdigest()
    
    def _load_state(self) -> Dict:
        """Load pipeline state for idempotent execution."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {{"last_hash": None, "last_run": None}}
    
    def _save_state(self, data_hash: str):
        """Save pipeline state."""
        state = {{
            "last_hash": data_hash,
            "last_run": datetime.now().isoformat()
        }}
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
    
    def extract(self, source_path: str) -> pd.DataFrame:
        """Extract data from source."""
        logger.info("Extracting data", source=source_path)
        try:
            source_format = "{source.get('format', 'csv')}"
            
            if source_format == "csv":
                df = pd.read_csv(source_path)
            elif source_format == "json":
                df = pd.read_json(source_path)
            elif source_format in ["excel", "xlsx", "xls"]:
                df = pd.read_excel(source_path)
            elif source_format == "parquet":
                df = pd.read_parquet(source_path)
            else:
                df = pd.read_csv(source_path)  # default to CSV
            
            logger.info("Data extracted", rows=len(df), columns=len(df.columns))
            return df
            
        except Exception as e:
            logger.error("Extraction failed", error=str(e))
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to the data."""
        logger.info("Starting transformations", input_rows=len(df))
        
        try:
'''
        
        # Add transformation steps
        for i, transform in enumerate(transformations):
            t_type = transform.get('type', 'map')
            t_desc = transform.get('description', '')
            
            code += f'''
            # Step {i+1}: {t_desc}
            logger.info("Applying transformation", step={i+1}, type="{t_type}")
'''
            
            if t_type == 'validate':
                code += '''
            # Remove rows with all null values
            df = df.dropna(how='all')
            # Fill remaining nulls with appropriate defaults
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].fillna(df[col].median())
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].fillna('UNKNOWN')
'''
            elif t_type == 'deduplicate':
                code += '''
            # Remove duplicate rows
            initial_count = len(df)
            df = df.drop_duplicates()
            logger.info("Deduplication complete", removed=initial_count - len(df))
'''
            elif t_type == 'filter':
                code += '''
            # Apply filtering (customize conditions as needed)
            # TODO: add actual filter conditions here
            # df = df[df['column_name'] > threshold]
            pass
'''
            elif t_type == 'normalize':
                code += '''
            # Normalize numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                min_val = df[col].min()
                max_val = df[col].max()
                if max_val > min_val:
                    df[col] = (df[col] - min_val) / (max_val - min_val)
'''
            elif t_type == 'aggregate':
                code += '''
            # Aggregation (customize groupby columns and aggregations)
            # df = df.groupby(['group_col']).agg({{'value_col': 'sum'}}).reset_index()
            pass  # need to specify the grouping logic
'''
            else:
                code += '''
            # Custom transformation - needs implementation
            pass
'''
        
        code += f'''
            self.metrics["records_processed"] = len(df)
            logger.info("Transformations complete", output_rows=len(df))
            return df
            
        except Exception as e:
            logger.error("Transformation failed", error=str(e))
            raise
    
    def load(self, df: pd.DataFrame, destination_path: str):
        """Load data to destination."""
        logger.info("Loading data", destination=destination_path)
        
        try:
            dest_format = "{destination.get('format', 'csv')}"
            
            if dest_format == "csv":
                df.to_csv(destination_path, index=False)
            elif dest_format == "json":
                df.to_json(destination_path, orient='records', indent=2)
            elif dest_format == "parquet":
                df.to_parquet(destination_path, index=False)
            elif dest_format in ["excel", "xlsx"]:
                df.to_excel(destination_path, index=False)
            else:
                df.to_csv(destination_path, index=False)
            
            logger.info("Data loaded successfully", rows=len(df))
            
        except Exception as e:
            logger.error("Loading failed", error=str(e))
            raise
    
    def run(self, source_path: str, destination_path: str, force: bool = False) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source_path: Path to source data
            destination_path: Path for output data
            force: Force execution even if data unchanged
            
        Returns:
            Dictionary with execution metrics
        """
        self.metrics["start_time"] = datetime.now().isoformat()
        logger.info("Pipeline execution started", 
                   source=source_path, 
                   destination=destination_path)
        
        try:
            # Extract
            df = self.extract(source_path)
            
            # Check for idempotency
            data_hash = self._compute_hash(df)
            state = self._load_state()
            
            if not force and state["last_hash"] == data_hash:
                logger.info("Data unchanged, skipping execution")
                return {{"status": "skipped", "reason": "data_unchanged"}}
            
            # Transform
            df = self.transform(df)
            
            # Load
            self.load(df, destination_path)
            
            # Save state
            self._save_state(data_hash)
            
            self.metrics["end_time"] = datetime.now().isoformat()
            self.metrics["status"] = "success"
            
            logger.info("Pipeline execution completed", metrics=self.metrics)
            return self.metrics
            
        except Exception as e:
            self.metrics["end_time"] = datetime.now().isoformat()
            self.metrics["status"] = "failed"
            self.metrics["error"] = str(e)
            logger.error("Pipeline execution failed", error=str(e))
            raise


def main():
    """Main entry point for the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="{pipeline_name}")
    parser.add_argument("--source", "-s", required=True, help="Source data path")
    parser.add_argument("--destination", "-d", required=True, help="Destination path")
    parser.add_argument("--force", "-f", action="store_true", help="Force execution")
    
    args = parser.parse_args()
    
    pipeline = {pipeline_name.title().replace('_', '')}Pipeline()
    result = pipeline.run(args.source, args.destination, args.force)
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
'''
        
        return code
    
    def _fallback_recommendations(self, schema_info: Dict, quality_metrics: Dict) -> List[Dict]:
        """Generate basic recommendations when AI is unavailable."""
        recommendations = []
        
        # Check for null values
        if quality_metrics.get('null_percentage', 0) > 5:
            recommendations.append({
                "type": "validation",
                "priority": "high",
                "title": "Handle Missing Values",
                "description": f"Found {quality_metrics.get('null_percentage', 0):.1f}% null values. Consider imputation or removal strategies."
            })
        
        # Check for duplicates
        if quality_metrics.get('duplicate_percentage', 0) > 0:
            recommendations.append({
                "type": "transformation",
                "priority": "medium",
                "title": "Remove Duplicates",
                "description": f"Found {quality_metrics.get('duplicate_percentage', 0):.1f}% duplicate rows. Add deduplication step."
            })
        
        # Check data types - this could probably be smarter
        if schema_info:
            for col, dtype in schema_info.get('columns', {}).items():
                if dtype == 'object':
                    recommendations.append({
                        "type": "optimization",
                        "priority": "low",
                        "title": f"Optimize Column: {col}",
                        "description": f"Column '{col}' is stored as object type. Consider converting to categorical or specific type."
                    })
        
        if not recommendations:
            recommendations.append({
                "type": "optimization",
                "priority": "low",
                "title": "Data Quality Good",
                "description": "No significant issues detected. Consider adding monitoring for production use."
            })
        
        return recommendations