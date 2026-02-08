"""
Analyzes data sources to detect schema, types, and quality metrics.
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import structlog

logger = structlog.get_logger()


def convert_to_serializable(obj):
    """
    Convert numpy/pandas types to JSON-serializable Python types.
    """
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return [convert_to_serializable(x) for x in obj.tolist()]
    elif isinstance(obj, (pd.Timestamp, np.datetime64)):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(x) for x in obj]
    elif pd.isna(obj):
        return None
    return obj


class SourceAnalyzer:
    """
    Analyzes various data sources to extract schema information and quality metrics.
    Supports CSV, JSON, Excel, and Parquet files.
    """
    
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.json': 'json',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.parquet': 'parquet',
        '.pq': 'parquet'
    }
    
    def __init__(self):
        """Initialize the Source Analyzer."""
        self.current_df = None
        self.current_path = None
        logger.info("Source Analyzer initialized")
    
    def detect_format(self, file_path: str) -> str:
        path = Path(file_path)
        ext = path.suffix.lower()
        
        detected = self.SUPPORTED_FORMATS.get(ext, 'unknown')
        logger.info("Format detected", file=path.name, format=detected)
        
        return detected
    
    def load_data(self, file_path: str, sample_size: int = None) -> pd.DataFrame:
        file_format = self.detect_format(file_path)
        
        try:
            if file_format == 'csv':
                df = pd.read_csv(file_path, nrows=sample_size)
            elif file_format == 'json':
                df = pd.read_json(file_path)
                if sample_size:
                    df = df.head(sample_size)
            elif file_format == 'excel':
                df = pd.read_excel(file_path, nrows=sample_size)
            elif file_format == 'parquet':
                df = pd.read_parquet(file_path)
                if sample_size:
                    df = df.head(sample_size)
            else:
                # Try CSV as fallback
                df = pd.read_csv(file_path, nrows=sample_size)
            
            self.current_df = df
            self.current_path = file_path
            
            logger.info("Data loaded", rows=len(df), columns=len(df.columns))
            return df
            
        except Exception as e:
            logger.error("Failed to load data", error=str(e), file=file_path)
            raise ValueError(f"Could not load file: {str(e)}")
    
    def infer_schema(self, df: pd.DataFrame = None) -> Dict[str, Any]:
        if df is None:
            df = self.current_df
        
        if df is None:
            raise ValueError("No data loaded. Call load_data first.")
        
        schema = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
            "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024)
        }
        
        for col in df.columns:
            # Get sample values and convert to serializable types
            sample_vals = df[col].dropna().head(3).tolist()
            sample_vals = [convert_to_serializable(v) for v in sample_vals]
            
            col_info = {
                "dtype": str(df[col].dtype),
                "nullable": bool(df[col].isnull().any()),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique()),
                "sample_values": sample_vals
            }
            
            # Add statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None,
                    "std": float(df[col].std()) if not pd.isna(df[col].std()) else None
                })
            
            # Add info for string columns
            elif df[col].dtype == 'object':
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    col_info.update({
                        "avg_length": float(non_null.str.len().mean()),
                        "max_length": int(non_null.str.len().max())
                    })
            
            schema["columns"][col] = col_info
        
        logger.info("Schema inferred", columns=len(schema["columns"]))
        return schema
    
    def calculate_quality_metrics(self, df: pd.DataFrame = None) -> Dict[str, Any]:
        if df is None:
            df = self.current_df
        
        if df is None:
            raise ValueError("No data loaded. Call load_data first.")
        
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        duplicate_rows = df.duplicated().sum()
        
        metrics = {
            "completeness": {
                "total_cells": int(total_cells),
                "null_cells": int(null_cells),
                "null_percentage": round((null_cells / total_cells) * 100, 2) if total_cells > 0 else 0,
                "complete_rows": int(len(df.dropna())),
                "complete_row_percentage": round((len(df.dropna()) / len(df)) * 100, 2) if len(df) > 0 else 0
            },
            "uniqueness": {
                "total_rows": int(len(df)),
                "duplicate_rows": int(duplicate_rows),
                "duplicate_percentage": round(float(duplicate_rows / len(df)) * 100, 2) if len(df) > 0 else 0,
                "unique_rows": int(len(df) - duplicate_rows)
            },
            "column_quality": {}
        }
        
        # Per-column quality metrics
        for col in df.columns:
            col_metrics = {
                "null_percentage": round((df[col].isnull().sum() / len(df)) * 100, 2),
                "unique_percentage": round((df[col].nunique() / len(df)) * 100, 2),
                "is_potential_id": df[col].nunique() == len(df) and not df[col].isnull().any()
            }
            
            # Check for potential issues
            if pd.api.types.is_numeric_dtype(df[col]):
                # Check for outliers using IQR method
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
                col_metrics["outlier_count"] = int(outliers)
                col_metrics["outlier_percentage"] = round((outliers / len(df)) * 100, 2)
            
            metrics["column_quality"][col] = col_metrics
        
        # Overall quality score (0-100) - simple average for now
        completeness_score = 100 - metrics["completeness"]["null_percentage"]
        uniqueness_score = 100 - metrics["uniqueness"]["duplicate_percentage"]
        metrics["overall_quality_score"] = round((completeness_score + uniqueness_score) / 2, 1)
        
        logger.info("Quality metrics calculated", score=metrics["overall_quality_score"])
        return metrics
    
    def analyze(self, file_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        logger.info("Starting source analysis", file=file_path)
        
        # Load data
        df = self.load_data(file_path)
        
        # Analyze
        schema = self.infer_schema(df)
        quality = self.calculate_quality_metrics(df)
        
        # Add file metadata
        file_info = {
            "file_path": str(file_path),
            "file_name": Path(file_path).name,
            "file_format": self.detect_format(file_path),
            "file_size_mb": round(Path(file_path).stat().st_size / (1024 * 1024), 2)
        }
        
        schema["file_info"] = file_info
        
        logger.info("Source analysis complete", 
                   file=file_info["file_name"],
                   quality_score=quality["overall_quality_score"])
        
        return schema, quality
    
    def get_preview(self, file_path: str = None, rows: int = 10) -> Dict[str, Any]:
        if file_path:
            df = self.load_data(file_path, sample_size=rows)
        else:
            df = self.current_df
        
        if df is None:
            raise ValueError("No data available for preview")
        
        preview_df = df.head(rows)
        
        # Convert data to JSON-serializable format
        preview_data = []
        for _, row in preview_df.iterrows():
            # TODO: there's probably a more efficient way to do this
            preview_data.append([convert_to_serializable(v) if not pd.isna(v) else "NULL" for v in row])
        
        return {
            "columns": list(preview_df.columns),
            "data": preview_data,
            "dtypes": {col: str(dtype) for col, dtype in preview_df.dtypes.items()},
            "total_rows": int(len(df)),
            "preview_rows": int(len(preview_df))
        }
    
    def suggest_transformations(self, schema: Dict, quality: Dict) -> list:
        suggestions = []
        
        # Check for null values
        if quality["completeness"]["null_percentage"] > 0:
            high_null_cols = [
                col for col, metrics in quality["column_quality"].items()
                if metrics["null_percentage"] > 10
            ]
            if high_null_cols:
                suggestions.append({
                    "type": "validate",
                    "reason": f"High null percentage in columns: {', '.join(high_null_cols[:3])}",
                    "action": "Handle missing values through imputation or removal"
                })
        
        # Check for duplicates
        if quality["uniqueness"]["duplicate_percentage"] > 0:
            suggestions.append({
                "type": "deduplicate",
                "reason": f"{quality['uniqueness']['duplicate_percentage']}% duplicate rows found",
                "action": "Remove duplicate rows"
            })
        
        # Check for potential normalization
        for col, info in schema["columns"].items():
            if info["dtype"] in ['int64', 'float64']:
                if info.get("max") and info.get("min"):
                    range_val = info["max"] - info["min"]
                    # arbitrary threshold but seems to work
                    if range_val > 1000:
                        suggestions.append({
                            "type": "normalize",
                            "reason": f"Column '{col}' has large value range ({range_val:.0f})",
                            "action": "Consider normalizing for ML applications"
                        })
                        break  # Only suggest once
        
        # Check for outliers
        for col, metrics in quality["column_quality"].items():
            if metrics.get("outlier_percentage", 0) > 5:
                suggestions.append({
                    "type": "filter",
                    "reason": f"Column '{col}' has {metrics['outlier_percentage']}% outliers",
                    "action": "Consider filtering or capping outlier values"
                })
                break  # Only suggest once
        
        logger.info("Transformation suggestions generated", count=len(suggestions))
        return suggestions


# Convenience function for quick analysis
def analyze_file(file_path: str) -> Dict[str, Any]:
    analyzer = SourceAnalyzer()
    schema, quality = analyzer.analyze(file_path)
    suggestions = analyzer.suggest_transformations(schema, quality)
    preview = analyzer.get_preview(rows=5)
    
    return {
        "schema": schema,
        "quality": quality,
        "suggestions": suggestions,
        "preview": preview
    }